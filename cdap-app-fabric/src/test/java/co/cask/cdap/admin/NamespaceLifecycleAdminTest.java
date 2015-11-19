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

import co.cask.cdap.common.NamespaceAlreadyExistsException;
import co.cask.cdap.common.NamespaceNotFoundException;
import co.cask.cdap.common.NotFoundException;
import co.cask.cdap.internal.app.services.http.AppFabricTestBase;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.NamespaceMeta;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for {@link NamespaceLifecycleAdmin}
 */
public class NamespaceLifecycleAdminTest extends AppFabricTestBase {

  private static final InstanceAdmin instanceAdmin = getInjector().getInstance(InstanceAdmin.class);
  private static final NamespaceLifecycleAdminFactory nsFactory =
    getInjector().getInstance(NamespaceLifecycleAdminFactory.class);

  @Test
  public void testNamespaces() throws Exception {
    String namespace = "namespace";
    Id.Namespace namespaceId = Id.Namespace.from(namespace);
    NamespaceLifecycleAdmin ns = nsFactory.create(namespaceId);

    int initialCount = instanceAdmin.listNamespaces().size();

    // TEST_NAMESPACE_META1 is already created in AppFabricTestBase#beforeClass
    NamespaceLifecycleAdmin test1 = nsFactory.create(Id.Namespace.from(TEST_NAMESPACE1));
    Assert.assertTrue(test1.exists());
    try {
      test1.create(TEST_NAMESPACE_META1.getDescription());
      Assert.fail("Should not create duplicate namespace.");
    } catch (NamespaceAlreadyExistsException e) {
      Assert.assertEquals(Id.Namespace.from(TEST_NAMESPACE_META1.getName()), e.getId());
    }

    // "random" namespace should not exist
    try {
      NamespaceLifecycleAdmin random = nsFactory.create(Id.Namespace.from("random"));
      random.get();
      Assert.fail("Namespace 'random' should not exist.");
    } catch (NamespaceNotFoundException e) {
      Assert.assertEquals(Id.Namespace.from("random"), e.getObject());
    }

    Assert.assertEquals(initialCount, instanceAdmin.listNamespaces().size());
    Assert.assertFalse(ns.exists());

    try {
      nsFactory.create(Id.Namespace.from(null));
      Assert.fail("Namespace with no name should fail");
    } catch (IllegalArgumentException e) {
      Assert.assertEquals("Namespace id cannot be null.", e.getMessage());
    }

    try {
      nsFactory.create(null);
      Assert.fail("Namespace with no name should fail");
    } catch (Exception e) {
      // expected
    }

    Assert.assertEquals(initialCount, instanceAdmin.listNamespaces().size());
    Assert.assertFalse(ns.exists());

    // namespace with default fields
    ns.create(null);
    Assert.assertEquals(initialCount + 1, instanceAdmin.listNamespaces().size());
    Assert.assertTrue(ns.exists());
    try {
      NamespaceMeta namespaceMeta = ns.get();
      Assert.assertEquals(namespaceId.getId(), namespaceMeta.getName());
      Assert.assertEquals("", namespaceMeta.getDescription());

      ns.delete();
    } catch (NotFoundException e) {
      Assert.fail(String.format("Namespace '%s' should be found since it was just created.", namespaceId.getId()));
    }

    ns.create("describes " + namespace);
    Assert.assertEquals(initialCount + 1, instanceAdmin.listNamespaces().size());
    Assert.assertTrue(ns.exists());

    try {
      NamespaceMeta namespaceMeta = ns.get();
      Assert.assertEquals(namespaceId.getId(), namespaceMeta.getName());
      Assert.assertEquals("describes " + namespaceId.getId(), namespaceMeta.getDescription());

      ns.delete();
    } catch (NotFoundException e) {
      Assert.fail(String.format("Namespace '%s' should be found since it was just created.", namespaceId.getId()));
    }

    // Verify NotFoundException's contents as well, instead of just checking namespaceService.exists = false
    verifyNotFound(namespaceId);
  }

  private static void verifyNotFound(Id.Namespace namespaceId) throws Exception {
    try {
      nsFactory.create(namespaceId).get();
      Assert.fail(String.format("Namespace '%s' should not be found since it was just deleted", namespaceId.getId()));
    } catch (NamespaceNotFoundException e) {
      Assert.assertEquals(Id.Namespace.from(namespaceId.getId()), e.getId());
    }
  }
}
