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

package co.cask.cdap.proto;

import javax.annotation.Nullable;

/**
 * Array components of the batch status request to POST /namespaces/{namespace}/instances.
 */
public class BatchRunnable extends BatchProgram {
  protected final String runnableId;

  public BatchRunnable(String appId, ProgramType programType, String programId, @Nullable String runnableId) {
    super(appId, programType, programId);
    this.runnableId = runnableId;
  }

  @Nullable
  public String getRunnableId() {
    return runnableId;
  }
}
