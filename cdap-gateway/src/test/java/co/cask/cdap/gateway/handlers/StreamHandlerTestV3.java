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

package co.cask.cdap.gateway.handlers;

import co.cask.cdap.common.conf.Constants;

import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;

/**
 * Tests v3 stream endpoints with default namespace
 */
public class StreamHandlerTestV3 extends StreamHandlerTest {
  @Override
  protected URL constructPath(String path) throws URISyntaxException, MalformedURLException {
    return getEndPoint(String.format("/v3/namespaces/%s/%s", Constants.DEFAULT_NAMESPACE, path)).toURL();
  }
}
