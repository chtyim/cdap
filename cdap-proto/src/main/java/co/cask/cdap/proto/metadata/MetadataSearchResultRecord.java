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
package co.cask.cdap.proto.metadata;

import co.cask.cdap.proto.Id;

import java.util.Objects;

/**
 * Represent the Metadata search result record.
 */
public class MetadataSearchResultRecord {
  private final Id.NamespacedId entityId;

  public MetadataSearchResultRecord(Id.NamespacedId entityId) {
    this.entityId = entityId;
  }

  public Id.NamespacedId getEntityId() {
    return entityId;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof MetadataSearchResultRecord)) {
      return false;
    }
    MetadataSearchResultRecord that = (MetadataSearchResultRecord) o;
    return Objects.equals(entityId, that.entityId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(entityId);
  }

  @Override
  public String toString() {
    return "MetadataSearchResultRecord{" +
      "entityId=" + entityId +
      '}';
  }
}
