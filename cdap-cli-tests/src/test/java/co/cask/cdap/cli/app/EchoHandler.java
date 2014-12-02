/*
 * Copyright © 2014 Cask Data, Inc.
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

package co.cask.cdap.cli.app;

import co.cask.cdap.api.service.http.AbstractHttpServiceHandler;
import co.cask.cdap.api.service.http.HttpServiceRequest;
import co.cask.cdap.api.service.http.HttpServiceResponder;

import javax.ws.rs.POST;
import javax.ws.rs.Path;

/**
 * Echo handler.
 */
public final class EchoHandler extends AbstractHttpServiceHandler {
  public static final String NAME = "echoHandler";

  @POST
  @Path("/echo")
  public void echo(HttpServiceRequest request, HttpServiceResponder responder) {
//    responder.send(200, request.getContent(), "text/plain", null);
    responder.sendStatus(200);
  }
}
