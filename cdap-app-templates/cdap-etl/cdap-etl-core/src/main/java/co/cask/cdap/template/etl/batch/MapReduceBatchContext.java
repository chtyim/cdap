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

package co.cask.cdap.template.etl.batch;

import co.cask.cdap.api.data.DatasetInstantiationException;
import co.cask.cdap.api.dataset.Dataset;
import co.cask.cdap.api.mapreduce.MapReduceContext;
import co.cask.cdap.api.metrics.Metrics;
import co.cask.cdap.api.templates.AdapterSpecification;
import co.cask.cdap.api.templates.plugins.PluginProperties;
import co.cask.cdap.template.etl.api.batch.BatchContext;
import co.cask.cdap.template.etl.common.Constants;

import java.util.Map;
import javax.annotation.Nullable;

/**
 * Abstract implementation of {@link BatchContext} using {@link MapReduceContext}.
 */
public abstract class MapReduceBatchContext extends BatchTransformContext implements BatchContext {

  protected final MapReduceContext mrContext;

  public MapReduceBatchContext(MapReduceContext context, Metrics metrics, String prefixId) {
    super(context, metrics, prefixId);
    this.mrContext = context;
  }

  @Override
  public long getLogicalStartTime() {
    return mrContext.getLogicalStartTime();
  }

  @Override
  public <T> T getHadoopJob() {
    return mrContext.getHadoopJob();
  }

  @Override
  public <T extends Dataset> T getDataset(String name) throws DatasetInstantiationException {
    return mrContext.getDataset(name);
  }

  @Override
  public <T extends Dataset> T getDataset(String name, Map<String, String> arguments)
    throws DatasetInstantiationException {
    return mrContext.getDataset(name, arguments);
  }

  @Nullable
  @Override
  public AdapterSpecification getAdapterSpecification() {
    return mrContext.getAdapterSpecification();
  }

  @Override
  public PluginProperties getPluginProperties(String pluginId) {
    // temporary hack until templates are removed, and to let this work for both apps and templates
    try {
      return mrContext.getPluginProperties(getPluginId(pluginId));
    } catch (UnsupportedOperationException e) {
      return mrContext.getPluginProps(getPluginId(pluginId));
    }
  }

  @Override
  public <T> Class<T> loadPluginClass(String pluginId) {
    // temporary hack until templates are removed, and to let this work for both apps and templates
    try {
      return mrContext.loadPluginClass(getPluginId(pluginId));
    } catch (UnsupportedOperationException e) {
      return mrContext.loadClass(getPluginId(pluginId));
    }
  }

  @Override
  public <T> T newPluginInstance(String pluginId) throws InstantiationException {
    // temporary hack until templates are removed, and to let this work for both apps and templates
    try {
      return mrContext.newPluginInstance(getPluginId(pluginId));
    } catch (UnsupportedOperationException e) {
      return mrContext.newInstance(getPluginId(pluginId));
    }
  }

  @Override
  public Map<String, String> getRuntimeArguments() {
    return mrContext.getRuntimeArguments();
  }

}
