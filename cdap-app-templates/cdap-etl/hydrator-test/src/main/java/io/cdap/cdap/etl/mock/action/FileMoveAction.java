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

package io.cdap.cdap.etl.mock.action;

import io.cdap.cdap.api.TxRunnable;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.DatasetContext;
import io.cdap.cdap.api.dataset.lib.FileSet;
import io.cdap.cdap.api.plugin.PluginClass;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.api.plugin.PluginPropertyField;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.action.Action;
import io.cdap.cdap.etl.api.action.ActionContext;
import io.cdap.cdap.etl.api.validation.InvalidConfigPropertyException;
import io.cdap.cdap.etl.api.validation.InvalidStageException;
import org.apache.twill.filesystem.Location;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import javax.annotation.Nullable;

/**
 * Action that moves files that match some regex.
 */
@Plugin(type = Action.PLUGIN_TYPE)
@Name(FileMoveAction.NAME)
public class FileMoveAction extends Action {
  public static final String NAME = "FileMove";
  public static final PluginClass PLUGIN_CLASS = getPluginClass();
  private final Conf conf;

  public FileMoveAction(Conf conf) {
    this.conf = conf;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    pipelineConfigurer.createDataset(conf.destinationFileset, FileSet.class);
    List<InvalidStageException> errors = new ArrayList<>();
    try {
      Pattern.compile(conf.filterRegex);
    } catch (Exception e) {
      errors.add(new InvalidConfigPropertyException(e.getMessage(), "filterRegex"));
    }
    if (conf.sourceFileset.equals(conf.destinationFileset)) {
      errors.add(new InvalidStageException("source and destination filesets must be different"));
    }
    if (!errors.isEmpty()) {
      throw new InvalidStageException(errors);
    }
  }

  @Override
  public void run(ActionContext context) throws Exception {
    context.execute(new TxRunnable() {
      @Override
      public void run(DatasetContext context) throws Exception {
        FileSet sourceFileSet = context.getDataset(conf.sourceFileset);
        FileSet destinationFileSet = context.getDataset(conf.destinationFileset);

        Pattern pattern = Pattern.compile(conf.filterRegex);

        for (Location sourceFile : sourceFileSet.getBaseLocation().list()) {
          if (pattern.matcher(sourceFile.getName()).matches()) {
            Location destFile = destinationFileSet.getBaseLocation().append(sourceFile.getName());
            sourceFile.renameTo(destFile);
          }
        }
      }
    });
  }

  /**
   * Conf for the token writer.
   */
  public static class Conf extends PluginConfig {
    private String sourceFileset;

    private String destinationFileset;

    @Nullable
    private String filterRegex;

    // set defaults for properties in a no-argument constructor.
    public Conf() {
      filterRegex = "^\\.";
    }
  }

  private static PluginClass getPluginClass() {
    Map<String, PluginPropertyField> properties = new HashMap<>();
    properties.put("sourceFileset", new PluginPropertyField("sourceFileset", "", "string", true, false));
    properties.put("destinationFileset", new PluginPropertyField("destinationFileset", "", "string", true, false));
    properties.put("filterRegex", new PluginPropertyField("filterRegex", "", "string", false, false));
    return new PluginClass(Action.PLUGIN_TYPE, NAME, "", FileMoveAction.class.getName(),
                           "conf", properties);
  }
}
