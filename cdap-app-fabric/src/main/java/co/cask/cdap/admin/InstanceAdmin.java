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

import co.cask.cdap.proto.NamespaceMeta;
import co.cask.cdap.store.NamespaceStore;
import com.google.inject.Inject;

import java.util.List;

/**
 * Administrates a CDAP instance.
 */
public class InstanceAdmin {

  private final NamespaceStore nsStore;

  @Inject
  public InstanceAdmin(NamespaceStore nsStore) {
    this.nsStore = nsStore;
  }

  /**
   * Lists all namespaces.
   *
   * @return a list of {@link NamespaceMeta} for all namespaces
   */
  public List<NamespaceMeta> listNamespaces() throws Exception {
    return nsStore.list();
  }

}
