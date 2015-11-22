/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.data2.metadata.system;

import co.cask.cdap.api.plugin.PluginClass;
import co.cask.cdap.data2.metadata.store.MetadataStore;
import co.cask.cdap.proto.Id;
import com.google.common.collect.ImmutableMap;

import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * A {@link AbstractSystemMetadataWriter} for an {@link Id.Artifact artifact}.
 */
public class ArtifactSystemMetadataWriter extends AbstractSystemMetadataWriter {

  private static final String PLUGIN_KEY_PREFIX = "plugin";

  private final Set<PluginClass> pluginClasses;

  public ArtifactSystemMetadataWriter(MetadataStore metadataStore, Id.Artifact artifactId,
                                      @Nullable Set<PluginClass> pluginClasses) {
    super(metadataStore, artifactId);
    this.pluginClasses = pluginClasses;
  }

  @Override
  Map<String, String> getSystemPropertiesToAdd() {
    ImmutableMap.Builder<String, String> properties = ImmutableMap.builder();
    if (pluginClasses != null) {
      for (PluginClass pluginClass : pluginClasses) {
        String name = pluginClass.getName();
        String type = pluginClass.getType();
        properties.put(PLUGIN_KEY_PREFIX + COLON + name, name + COLON + type);
      }
    }
    return properties.build();
  }

  @Override
  String[] getSystemTagsToAdd() {
    return new String[0];
  }
}
