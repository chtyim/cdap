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

import co.cask.cdap.common.NamespaceNotFoundException;
import co.cask.cdap.common.NotFoundException;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.NamespaceConfig;
import co.cask.cdap.proto.NamespaceMeta;
import co.cask.cdap.store.NamespaceStore;
import com.google.common.base.Strings;
import com.google.inject.Inject;
import com.google.inject.assistedinject.Assisted;

/**
 * Admin for basic namespace operations on a single namespace.
 */
public class NamespaceAdmin {

  protected final Id.Namespace id;
  protected final NamespaceStore nsStore;

  @Inject
  NamespaceAdmin(@Assisted Id.Namespace id, NamespaceStore nsStore) {
    this.id = id;
    this.nsStore = nsStore;
  }

  public Id.Namespace getId() {
    return id;
  }

  /**
   * @return the {@link NamespaceMeta} of the namespace
   * @throws NamespaceNotFoundException if the namespace is not found
   */
  public NamespaceMeta get() throws NamespaceNotFoundException {
    NamespaceMeta ns = nsStore.get(id);
    if (ns == null) {
      throw new NamespaceNotFoundException(id);
    }
    return ns;
  }

  /**
   * @return true if the namespace exists
   */
  public boolean exists() {
    try {
      get();
    } catch (NotFoundException e) {
      return false;
    }
    return true;
  }

  /**
   * Updates the properties of the namespace.
   *
   * @param namespaceMeta the new namespace properties
   * @throws NamespaceNotFoundException if the namespace is not found
   */
  public synchronized void updateProperties(NamespaceMeta namespaceMeta)
    throws NamespaceNotFoundException {

    if (nsStore.get(id) == null) {
      throw new NamespaceNotFoundException(id);
    }
    NamespaceMeta metadata = nsStore.get(id);
    NamespaceMeta.Builder builder = new NamespaceMeta.Builder(metadata);

    if (namespaceMeta.getDescription() != null) {
      builder.setDescription(namespaceMeta.getDescription());
    }

    NamespaceConfig config = namespaceMeta.getConfig();
    if (config != null && !Strings.isNullOrEmpty(config.getSchedulerQueueName())) {
      builder.setSchedulerQueueName(config.getSchedulerQueueName());
    }

    nsStore.update(builder.build());
  }
}
