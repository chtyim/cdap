/*
 * Copyright © 2014-2015 Cask Data, Inc.
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

package co.cask.cdap.data2.dataset2;

import co.cask.cdap.api.dataset.Dataset;
import co.cask.cdap.api.dataset.DatasetAdmin;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.api.dataset.module.DatasetModule;
import co.cask.cdap.proto.DatasetSpecificationSummary;
import co.cask.cdap.proto.Id;
import com.google.common.collect.ImmutableList;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Wrapper for {@link DatasetFramework} that namespaces dataset instances names.
 */
public class NamespacedDatasetFramework implements DatasetFramework {

  private final DatasetNamespace namespace;
  private final DatasetFramework delegate;

  public NamespacedDatasetFramework(DatasetFramework delegate, DatasetNamespace namespace) {
    this.delegate = delegate;
    this.namespace = namespace;
  }

  @Override
  public void addModule(Id.DatasetModule moduleId, DatasetModule module)
    throws DatasetManagementException {

    delegate.addModule(moduleId, module);
  }

  @Override
  public void deleteModule(Id.DatasetModule moduleId) throws DatasetManagementException {
    delegate.deleteModule(moduleId);
  }

  @Override
  public void deleteAllModules(Id.Namespace namespaceId) throws DatasetManagementException {
    delegate.deleteAllModules(namespaceId);
  }

  @Override
  public void addInstance(String datasetType, Id.DatasetInstance datasetInstanceId, DatasetProperties props)
    throws DatasetManagementException, IOException {

    delegate.addInstance(datasetType, namespace(datasetInstanceId), props);
  }

  @Override
  public void updateInstance(Id.DatasetInstance datasetInstanceId, DatasetProperties props)
    throws DatasetManagementException, IOException {
    delegate.updateInstance(namespace(datasetInstanceId), props);
  }

  @Override
  public Collection<DatasetSpecificationSummary> getInstances(Id.Namespace namespaceId)
    throws DatasetManagementException {
    Collection<DatasetSpecificationSummary> specs = delegate.getInstances(namespaceId);
    // client may pass the name back e.g. do delete instance, so we need to un-namespace it
    ImmutableList.Builder<DatasetSpecificationSummary> builder = ImmutableList.builder();
    for (DatasetSpecificationSummary spec : specs) {
      DatasetSpecificationSummary s = fromNamespaced(namespaceId, spec);
      if (s != null) {
        builder.add(s);
      }
    }
    return builder.build();
  }

  @Nullable
  @Override
  public DatasetSpecification getDatasetSpec(Id.DatasetInstance datasetInstanceId) throws DatasetManagementException {
    return fromNamespaced(datasetInstanceId.getNamespace(), delegate.getDatasetSpec(namespace(datasetInstanceId)));
  }

  @Override
  public boolean hasInstance(Id.DatasetInstance datasetInstanceId) throws DatasetManagementException {
    return delegate.hasInstance(namespace(datasetInstanceId));
  }

  @Override
  public boolean hasSystemType(String typeName) throws DatasetManagementException {
    return delegate.hasSystemType(typeName);
  }

  @Override
  public boolean hasType(Id.DatasetType datasetTypeId) throws DatasetManagementException {
    return delegate.hasType(datasetTypeId);
  }

  @Override
  public void deleteInstance(Id.DatasetInstance datasetInstanceId) throws DatasetManagementException, IOException {
    delegate.deleteInstance(namespace(datasetInstanceId));
  }

  @Override
  public void deleteAllInstances(Id.Namespace namespaceId) throws DatasetManagementException, IOException {
    // delete all instances ONLY in this namespace
    for (DatasetSpecificationSummary spec : getInstances(namespaceId)) {
      Id.DatasetInstance datasetInstanceId = Id.DatasetInstance.from(namespaceId, spec.getName());
      deleteInstance(datasetInstanceId);
    }
  }

  @Override
  public <T extends DatasetAdmin> T getAdmin(Id.DatasetInstance datasetInstanceId, ClassLoader classLoader)
    throws DatasetManagementException, IOException {

    return delegate.getAdmin(namespace(datasetInstanceId), classLoader);
  }

  @Override
  public <T extends Dataset> T getDataset(Id.DatasetInstance datasetInstanceId, Map<String, String> arguments,
                                          ClassLoader classLoader)
    throws DatasetManagementException, IOException {

    return delegate.getDataset(namespace(datasetInstanceId), arguments, classLoader);
  }

  @Override
  public void createNamespace(Id.Namespace namespaceId) throws DatasetManagementException {
    delegate.createNamespace(namespaceId);
  }

  @Override
  public void deleteNamespace(Id.Namespace namespaceId) throws DatasetManagementException {
    delegate.deleteNamespace(namespaceId);
  }

  @Nullable
  private DatasetSpecification fromNamespaced(Id.Namespace namespaceId, @Nullable DatasetSpecification spec) {
    if (spec == null) {
      return null;
    }
    Id.DatasetInstance namespaced = Id.DatasetInstance.from(namespaceId, spec.getName());
    Id.DatasetInstance notNamespaced = namespace.fromNamespaced(namespaced);
    if (notNamespaced == null) {
      return null;
    }
    return DatasetSpecification.changeName(spec, notNamespaced.getId());
  }

  @Nullable
  private DatasetSpecificationSummary fromNamespaced(Id.Namespace namespaceId, DatasetSpecificationSummary spec) {
    if (spec == null) {
      return null;
    }
    Id.DatasetInstance namespaced = Id.DatasetInstance.from(namespaceId, spec.getName());
    Id.DatasetInstance notNamespaced = namespace.fromNamespaced(namespaced);
    if (notNamespaced == null) {
      return null;
    }
    return new DatasetSpecificationSummary(notNamespaced.getId(), spec.getType(), spec.getProperties());
  }

  private Id.DatasetInstance namespace(Id.DatasetInstance datasetInstanceId) {
    return namespace.namespace(datasetInstanceId);
  }
}
