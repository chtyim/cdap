/*
 * Copyright © 2015 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap.admin;

import co.cask.cdap.api.metrics.MetricDeleteQuery;
import co.cask.cdap.api.metrics.MetricStore;
import co.cask.cdap.app.runtime.ProgramRuntimeService;
import co.cask.cdap.app.store.Store;
import co.cask.cdap.common.NamespaceAlreadyExistsException;
import co.cask.cdap.common.NamespaceCannotBeCreatedException;
import co.cask.cdap.common.NamespaceCannotBeDeletedException;
import co.cask.cdap.common.NamespaceNotFoundException;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.config.DashboardStore;
import co.cask.cdap.config.PreferencesStore;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.DatasetManagementException;
import co.cask.cdap.data2.transaction.queue.QueueAdmin;
import co.cask.cdap.data2.transaction.stream.StreamAdmin;
import co.cask.cdap.internal.app.runtime.artifact.ArtifactRepository;
import co.cask.cdap.internal.app.runtime.schedule.Scheduler;
import co.cask.cdap.internal.app.services.ApplicationLifecycleService;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.NamespaceMeta;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.store.NamespaceStore;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import com.google.inject.assistedinject.Assisted;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

/**
 * Extension to {@link NamespaceAdmin} that supports create and delete.
 */
public class NamespaceLifecycleAdmin extends NamespaceAdmin {

  private static final Logger LOG = LoggerFactory.getLogger(NamespaceLifecycleAdmin.class);

  private final Store store;
  private final PreferencesStore preferencesStore;
  private final DashboardStore dashboardStore;
  private final DatasetFramework dsFramework;
  private final ProgramRuntimeService runtimeService;
  private final QueueAdmin queueAdmin;
  private final StreamAdmin streamAdmin;
  private final MetricStore metricStore;
  private final Scheduler scheduler;
  private final ApplicationLifecycleService applicationLifecycleService;
  private final ArtifactRepository artifactRepository;

  @Inject
  NamespaceLifecycleAdmin(@Assisted Id.Namespace id, NamespaceStore nsStore,
                          Store store, PreferencesStore preferencesStore,
                          DashboardStore dashboardStore, DatasetFramework dsFramework,
                          ProgramRuntimeService runtimeService, QueueAdmin queueAdmin, StreamAdmin streamAdmin,
                          MetricStore metricStore, Scheduler scheduler,
                          ApplicationLifecycleService applicationLifecycleService,
                          ArtifactRepository artifactRepository) {
    super(id, nsStore);
    this.queueAdmin = queueAdmin;
    this.streamAdmin = streamAdmin;
    this.store = store;
    this.preferencesStore = preferencesStore;
    this.dashboardStore = dashboardStore;
    this.dsFramework = dsFramework;
    this.runtimeService = runtimeService;
    this.scheduler = scheduler;
    this.metricStore = metricStore;
    this.applicationLifecycleService = applicationLifecycleService;
    this.artifactRepository = artifactRepository;
  }

  /**
   * Creates the namespace.
   *
   * @param description the description of the namespace
   * @param schedulerQueueName the scheduler queue name of the namespace
   * @throws NamespaceCannotBeCreatedException if the namespace cannot be created
   * @throws NamespaceAlreadyExistsException if the namespace already exists
   */
  public synchronized void create(String description, String schedulerQueueName)
    throws NamespaceCannotBeCreatedException, NamespaceAlreadyExistsException {

    NamespaceMeta metadata = new NamespaceMeta.Builder()
      .setName(id.getId())
      .setDescription(description)
      .setSchedulerQueueName(schedulerQueueName)
      .build();

    // TODO: CDAP-1427 - This should be transactional, but we don't support transactions on files yet
    Preconditions.checkArgument(metadata != null, "Namespace metadata should not be null.");
    Id.Namespace namespace = Id.Namespace.from(metadata.getName());
    if (exists()) {
      throw new NamespaceAlreadyExistsException(namespace);
    }

    try {
      dsFramework.createNamespace(Id.Namespace.from(metadata.getName()));
    } catch (DatasetManagementException e) {
      throw new NamespaceCannotBeCreatedException(namespace, e);
    }

    nsStore.create(metadata);
  }

  public void create(String description) throws NamespaceCannotBeCreatedException, NamespaceAlreadyExistsException {
    create(description, null);
  }

  /**
   * Deletes the namespace.
   *
   * @throws NamespaceCannotBeDeletedException if the namespace cannot be deleted
   * @throws NamespaceNotFoundException if the namespace is not found
   */
  public synchronized void delete()
    throws NamespaceCannotBeDeletedException, NamespaceNotFoundException {
    // TODO: CDAP-870, CDAP-1427: Delete should be in a single transaction.
    if (!exists()) {
      throw new NamespaceNotFoundException(id);
    }

    if (checkProgramsRunning()) {
      throw new NamespaceCannotBeDeletedException(
        id, String.format("Some programs are currently running in namespace " +
                            "'%s', please stop them before deleting namespace", id.getId()));
    }

    LOG.info("Deleting namespace '{}'.", id.getId());
    try {
      // Delete Preferences associated with this namespace
      preferencesStore.deleteProperties(id.getId());
      // Delete all dashboards associated with this namespace
      dashboardStore.delete(id.getId());
      // Delete datasets and modules
      dsFramework.deleteAllInstances(id);
      dsFramework.deleteAllModules(id);
      // Delete queues and streams data
      queueAdmin.dropAllInNamespace(id);
      streamAdmin.dropAllInNamespace(id);
      // Delete all the schedules
      scheduler.deleteAllSchedules(id);
      // Delete all applications
      applicationLifecycleService.removeAll(id);
      // Delete all meta data
      store.removeAll(id);

      deleteMetrics(id);
      // delete all artifacts in the namespace
      artifactRepository.clear(id);

      // Delete the namespace itself, only if it is a non-default namespace. This is because we do not allow users to
      // create default namespace, and hence deleting it may cause undeterministic behavior.
      // Another reason for not deleting the default namespace is that we do not want to call a delete on the default
      // namespace in the storage provider (Hive, HBase, etc), since we re-use their default namespace.
      if (!Id.Namespace.DEFAULT.equals(id)) {
        // Finally delete namespace from MDS
        nsStore.delete(id);
        // Delete namespace in storage providers
        dsFramework.deleteNamespace(id);
      }
    } catch (Exception e) {
      LOG.warn("Error while deleting namespace {}", id.getId(), e);
      throw new NamespaceCannotBeDeletedException(id, e);
    }
    LOG.info("All data for namespace '{}' deleted.", id.getId());
  }

  private void deleteMetrics(Id.Namespace namespaceId) throws Exception {
    long endTs = System.currentTimeMillis() / 1000;
    Map<String, String> tags = Maps.newHashMap();
    tags.put(Constants.Metrics.Tag.NAMESPACE, namespaceId.getId());
    MetricDeleteQuery deleteQuery = new MetricDeleteQuery(0, endTs, tags);
    metricStore.delete(deleteQuery);
  }

  /**
   * Deletes all datasets belonging to the namespace.
   *
   * @throws NamespaceNotFoundException if the namespace is not found
   * @throws NamespaceCannotBeDeletedException if the datasets cannot be deleted
   */
  public synchronized void deleteDatasets()
    throws NamespaceNotFoundException, NamespaceCannotBeDeletedException {
    // TODO: CDAP-870, CDAP-1427: Delete should be in a single transaction.
    if (!exists()) {
      throw new NamespaceNotFoundException(id);
    }

    if (checkProgramsRunning()) {
      throw new NamespaceCannotBeDeletedException(
        id, String.format("Some programs are currently running in namespace " +
                            "'%s', please stop them before deleting datasets " +
                            "in the namespace.", id.getId()));
    }

    try {
      dsFramework.deleteAllInstances(id);
    } catch (DatasetManagementException | IOException e) {
      LOG.warn("Error while deleting datasets in namespace {}", id.getId(), e);
      throw new NamespaceCannotBeDeletedException(id, e);
    }
    LOG.debug("Deleted datasets in namespace '{}'.", id.getId());
  }

  private boolean checkProgramsRunning() {
    return runtimeService.checkAnyRunning(new Predicate<Id.Program>() {
      @Override
      public boolean apply(Id.Program program) {
        return program.getNamespaceId().equals(id.getId());
      }
    }, ProgramType.values());
  }
}
