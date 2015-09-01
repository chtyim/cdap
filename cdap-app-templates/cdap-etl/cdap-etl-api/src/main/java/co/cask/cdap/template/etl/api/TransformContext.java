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

package co.cask.cdap.template.etl.api;

import co.cask.cdap.api.annotation.Beta;
import co.cask.cdap.api.metrics.Metrics;
import co.cask.cdap.api.templates.plugins.PluginProperties;

/**
 * Context passed to ETL stages.
 */
@Beta
public interface TransformContext {

  /**
   * Gets the {@link PluginProperties} associated with the stage.
   *
   * @return the {@link PluginProperties}.
   */
  PluginProperties getPluginProperties();

  /**
   * Get an instance of {@link Metrics}, used to collect metrics. Note that metric names are not scoped by
   * the stage they are emitted from. A metric called 'reads' emitted in one stage will be aggregated with
   * those emitted in another stage.
   *
   * @return {@link Metrics} for collecting metrics
   */
  Metrics getMetrics();

  /**
   * Creates a new instance of a plugin.
   * The instance returned will have the {@link co.cask.cdap.api.templates.plugins.PluginConfig} setup with
   * {@link PluginProperties} provided at the time when the
   * {@link co.cask.cdap.api.templates.AdapterConfigurer#usePlugin(String, String, String, PluginProperties)}
   * was called during the
   * {@link co.cask.cdap.api.templates.ApplicationTemplate#configureAdapter(String,
   * Object, co.cask.cdap.api.templates.AdapterConfigurer)} adapter configuration time.
   *
   * @param pluginId the unique identifier provide when declaring plugin usage in
   * {@link co.cask.cdap.api.templates.AdapterConfigurer}
   * @param <T> the class type of the plugin
   * @return A new instance of the plugin being specified by the arguments
   *
   * @throws InstantiationException if failed create a new instance.
   * @throws UnsupportedOperationException if the program is not running under the adapter context
   * @throws IllegalArgumentException if no plugin for the given type and name
   */
  <T> T newPluginInstance(String pluginId) throws InstantiationException;

  /**
   * Creates a new instance of a plugin.
   * The instance returned will have the {@link co.cask.cdap.api.templates.plugins.PluginConfig} setup with
   * {@link PluginProperties} provided at the time when the
   * {@link co.cask.cdap.api.artifact.PluginConfigurer#usePlugin(String, String, String, PluginProperties)}
   * was called during the program configuration time.
   *
   * @param pluginId the unique identifier provide when declaring plugin usage in the program
   * @param <T> the class type of the plugin
   * @return A new instance of the plugin being specified by the arguments
   *
   * @throws InstantiationException if failed create a new instance
   * @throws IllegalArgumentException if pluginId is not found
   * @throws UnsupportedOperationException if the program does not support plugin
   */
  <T> T newInstance(String pluginId) throws InstantiationException;
}
