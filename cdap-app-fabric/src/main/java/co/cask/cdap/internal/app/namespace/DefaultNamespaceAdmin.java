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

package co.cask.cdap.internal.app.namespace;

import co.cask.cdap.app.runtime.ProgramRuntimeService;
import co.cask.cdap.app.store.Store;
import co.cask.cdap.app.store.StoreFactory;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.exception.AlreadyExistsException;
import co.cask.cdap.common.exception.NotFoundException;
import co.cask.cdap.config.DashboardStore;
import co.cask.cdap.config.PreferencesStore;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.DatasetManagementException;
import co.cask.cdap.data2.transaction.queue.QueueAdmin;
import co.cask.cdap.data2.transaction.stream.StreamAdmin;
import co.cask.cdap.notifications.feeds.NotificationFeedException;
import co.cask.cdap.notifications.feeds.NotificationFeedManager;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.NamespaceConfig;
import co.cask.cdap.proto.NamespaceMeta;
import co.cask.cdap.proto.ProgramType;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Throwables;
import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

/**
 * Admin for managing namespaces
 */
public final class DefaultNamespaceAdmin implements NamespaceAdmin {
  private static final Logger LOG = LoggerFactory.getLogger(DefaultNamespaceAdmin.class);
  private static final String NAMESPACE_ELEMENT_TYPE = "Namespace";

  private final Store store;
  private final PreferencesStore preferencesStore;
  private final DashboardStore dashboardStore;
  private final DatasetFramework dsFramework;
  private final ProgramRuntimeService runtimeService;
  private final QueueAdmin queueAdmin;
  private final StreamAdmin streamAdmin;
  private final NotificationFeedManager notificationFeedManager;

  @Inject
  public DefaultNamespaceAdmin(StoreFactory storeFactory, PreferencesStore preferencesStore,
                               DashboardStore dashboardStore, DatasetFramework dsFramework,
                               ProgramRuntimeService runtimeService, QueueAdmin queueAdmin, StreamAdmin streamAdmin,
                               NotificationFeedManager notificationFeedManager) {
    this.queueAdmin = queueAdmin;
    this.streamAdmin = streamAdmin;
    this.store = storeFactory.create();
    this.preferencesStore = preferencesStore;
    this.dashboardStore = dashboardStore;
    this.dsFramework = dsFramework;
    this.runtimeService = runtimeService;
    this.notificationFeedManager = notificationFeedManager;
  }

  /**
   * This should be removed once we stop support for v2 APIs, since 'default' namespace is only reserved for v2 APIs.
   */
  private void createDefaultNamespace() {
    NamespaceMeta.Builder builder = new NamespaceMeta.Builder();
    NamespaceMeta defaultNamespace = builder.setId(Constants.DEFAULT_NAMESPACE)
      .setName("Default namespace")
      .setDescription("The default namespace, a reserved system namespace.")
      .build();

    try {
      createNamespace(defaultNamespace);
      LOG.info("Successfully created 'default' namespace.");
    } catch (AlreadyExistsException e) {
      LOG.info("'default' namespace already exists.");
    } catch (NamespaceCannotBeCreatedException e) {
      LOG.error("Error while creating default namespace", e);
      Throwables.propagate(e);
    }
  }

  /**
   * Lists all namespaces
   *
   * @return a list of {@link NamespaceMeta} for all namespaces
   */
  public List<NamespaceMeta> listNamespaces() {
    return store.listNamespaces();
  }

  /**
   * Gets details of a namespace
   *
   * @param namespaceId the {@link Id.Namespace} of the requested namespace
   * @return the {@link NamespaceMeta} of the requested namespace
   * @throws NotFoundException if the requested namespace is not found
   */
  public NamespaceMeta getNamespace(Id.Namespace namespaceId) throws NotFoundException {
    NamespaceMeta ns = store.getNamespace(namespaceId);
    if (ns == null) {
      throw new NotFoundException(NAMESPACE_ELEMENT_TYPE, namespaceId.getId());
    }
    return ns;
  }

  /**
   * Checks if the specified namespace exists
   *
   * @param namespaceId the {@link Id.Namespace} to check for existence
   * @return true, if the specifed namespace exists, false otherwise
   */
  public boolean hasNamespace(Id.Namespace namespaceId) {
    boolean exists = true;
    try {
      getNamespace(namespaceId);
    } catch (NotFoundException e) {
      // TODO: CDAP-1213 do this better
      if (Constants.DEFAULT_NAMESPACE.equals(namespaceId.getId())) {
        createDefaultNamespace();
      } else {
        exists = false;
      }
    }
    return exists;
  }

  /**
   * Creates a new namespace
   *
   * @param metadata the {@link NamespaceMeta} for the new namespace to be created
   * @throws AlreadyExistsException if the specified namespace already exists
   */
  public void createNamespace(NamespaceMeta metadata) throws NamespaceCannotBeCreatedException, AlreadyExistsException {
    // TODO: CDAP-1427 - This should be transactional, but we don't support transactions on files yet
    Preconditions.checkArgument(metadata != null, "Namespace metadata should not be null.");
    NamespaceMeta existing = store.getNamespace(Id.Namespace.from(metadata.getId()));
    if (existing != null) {
      throw new AlreadyExistsException(NAMESPACE_ELEMENT_TYPE, metadata.getId());
    }

    try {
      dsFramework.createNamespace(Id.Namespace.from(metadata.getId()));
    } catch (DatasetManagementException e) {
      throw new NamespaceCannotBeCreatedException(metadata.getId(), e);
    }

    store.createNamespace(metadata);
    String feedName = metadata.getName() + "_app-lifecycle";
    Id.NotificationFeed feed = new Id.NotificationFeed.Builder()
      .setNamespaceId(metadata.getName())
      .setCategory(Constants.Notification.APP_LIFECYCLE)
      .setName(feedName).build();

    try {
      notificationFeedManager.createFeed(feed);
    } catch (NotificationFeedException e) {
      LOG.error("Unable to create the notification feed {} {}", feedName, e);
    }
  }

  /**
   * Deletes the specified namespace
   *
   * @param namespaceId the {@link Id.Namespace} of the specified namespace
   * @throws NamespaceCannotBeDeletedException if the specified namespace cannot be deleted
   * @throws NotFoundException if the specified namespace does not exist
   */
  public void deleteNamespace(final Id.Namespace namespaceId)
    throws NamespaceCannotBeDeletedException, NotFoundException {
    // TODO: CDAP-870, CDAP-1427: Delete should be in a single transaction.
    if (!hasNamespace(namespaceId)) {
      throw new NotFoundException(NAMESPACE_ELEMENT_TYPE, namespaceId.getId());
    }

    if (areProgramsRunning(namespaceId)) {
      throw new NamespaceCannotBeDeletedException(namespaceId.getId(),
                                                  "Some programs are currently running in namespace " + namespaceId);
    }

    LOG.info("Deleting namespace '{}'.", namespaceId);
    try {
      // Delete Preferences associated with this namespace
      preferencesStore.deleteProperties(namespaceId.getId());
      // Delete all dashboards associated with this namespace
      dashboardStore.delete(namespaceId.getId());
      // Delete datasets and modules
      dsFramework.deleteAllInstances(namespaceId);
      dsFramework.deleteAllModules(namespaceId);
      // Delete queues and streams data
      queueAdmin.dropAllInNamespace(namespaceId.getId());
      streamAdmin.dropAllInNamespace(namespaceId);
      // Delete all meta data
      store.removeAll(namespaceId);
      // TODO: CDAP-1729 - Delete/Expire Metrics. API unavailable right now.

      // Delete the namespace itself, only if it is a non-default namespace. This is because we do not allow users to
      // create default namespace, and hence deleting it may cause undeterministic behavior.
      // Another reason for not deleting the default namespace is that we do not want to call a delete on the default
      // namespace in the storage provider (Hive, HBase, etc), since we re-use their default namespace.
      // This condition is only required to support the v2 unrecoverable reset API, since that uses NamespaceAdmin
      // directly. If you go through the Namespace delete REST API, this condition will already be met, since that
      // disallows deletion of reserved namespaces altogether.
      // TODO: Remove this check when the v2 unrecoverable reset API is removed
      if (!Constants.DEFAULT_NAMESPACE_ID.equals(namespaceId)) {
        // Delete namespace in storage providers
        dsFramework.deleteNamespace(namespaceId);
        // Finally delete namespace from MDS
        store.deleteNamespace(namespaceId);
      }
    } catch (DatasetManagementException e) {
      LOG.warn("Error while deleting namespace {}", namespaceId, e);
      throw new NamespaceCannotBeDeletedException(namespaceId.getId(), e);
    } catch (IOException e) {
      LOG.warn("Error while deleting namespace {}", namespaceId, e);
      throw new NamespaceCannotBeDeletedException(namespaceId.getId(), e);
    } catch (Exception e) {
      LOG.warn("Error while deleting namespace {}", namespaceId, e);
      throw new NamespaceCannotBeDeletedException(namespaceId.getId(), e);
    }
    LOG.info("All data for namespace '{}' deleted.", namespaceId);
  }

  @Override
  public void deleteDatasets(Id.Namespace namespaceId)
    throws NotFoundException, NamespaceCannotBeDeletedException {
    // TODO: CDAP-870, CDAP-1427: Delete should be in a single transaction.
    if (!hasNamespace(namespaceId)) {
      throw new NotFoundException(NAMESPACE_ELEMENT_TYPE, namespaceId.getId());
    }

    if (areProgramsRunning(namespaceId)) {
      throw new NamespaceCannotBeDeletedException(namespaceId.getId(),
                                                  "Some programs are currently running in namespace " + namespaceId);
    }

    LOG.info("Deleting data in namespace '{}'.", namespaceId);
    try {
      dsFramework.deleteAllInstances(namespaceId);
    } catch (DatasetManagementException e) {
      LOG.warn("Error while deleting data in namespace {}", namespaceId, e);
      throw new NamespaceCannotBeDeletedException(namespaceId.getId(), e);
    } catch (IOException e) {
      LOG.warn("Error while deleting data in namespace {}", namespaceId, e);
      throw new NamespaceCannotBeDeletedException(namespaceId.getId(), e);
    }
    LOG.info("Deleted data in namespace '{}'.", namespaceId);
  }

  public void updateProperties(Id.Namespace namespaceId, NamespaceMeta namespaceMeta) throws NotFoundException {
    if (store.getNamespace(namespaceId) == null) {
      throw new NotFoundException(NAMESPACE_ELEMENT_TYPE, namespaceId.getId());
    }
    NamespaceMeta metadata = store.getNamespace(namespaceId);
    NamespaceMeta.Builder builder = new NamespaceMeta.Builder(metadata);

    if (namespaceMeta.getDescription() != null) {
      builder.setDescription(namespaceMeta.getDescription());
    }

    if (namespaceMeta.getName() != null) {
      builder.setName(namespaceMeta.getName());
    }

    NamespaceConfig config = namespaceMeta.getConfig();

    if (config != null && config.getSchedulerQueueName() != null && !config.getSchedulerQueueName().isEmpty()) {
      builder.setSchedulerQueueName(config.getSchedulerQueueName());
    }

    store.updateNamespace(builder.build());
  }

  private boolean areProgramsRunning(final Id.Namespace namespaceId) {
    return runtimeService.checkAnyRunning(new Predicate<Id.Program>() {
      @Override
      public boolean apply(Id.Program program) {
        return program.getNamespaceId().equals(namespaceId.getId());
      }
    }, ProgramType.values());
  }
}
