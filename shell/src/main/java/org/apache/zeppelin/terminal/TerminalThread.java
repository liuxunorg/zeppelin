/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.zeppelin.terminal;

import org.apache.zeppelin.interpreter.InterpreterContext;
import org.apache.zeppelin.interpreter.InterpreterResult;
import org.apache.zeppelin.interpreter.InterpreterResultMessageOutput;
import javax.websocket.server.ServerContainer;

import org.apache.zeppelin.terminal.websocket.TerminalSocket;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.handler.HandlerCollection;
import org.eclipse.jetty.server.handler.ResourceHandler;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.websocket.jsr356.server.deploy.WebSocketServerContainerInitializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URL;

public class TerminalThread extends Thread {
  private static final Logger LOGGER = LoggerFactory.getLogger(TerminalThread.class);

  private Server jettyServer = new Server();

  private int port = 0;

  public TerminalThread(int port) {
    this.port = port;
  }

  public void run() {
    ServerConnector connector = new ServerConnector(jettyServer);
    connector.setPort(port);
    jettyServer.addConnector(connector);

    ServletContextHandler context = new ServletContextHandler(ServletContextHandler.SESSIONS);
    context.setContextPath("/terminal/");

    // We look for a file, as ClassLoader.getResource() is not
    // designed to look for directories (we resolve the directory later)
    ClassLoader clazz = TerminalThread.class.getClassLoader();
    URL url = clazz.getResource("html");
    if (url == null) {
      throw new RuntimeException("Unable to find resource directory");
    }

    ResourceHandler resourceHandler = new ResourceHandler();
    // Resolve file to directory
    String webRootUri = url.toExternalForm();
    LOGGER.info("WebRoot is " + webRootUri);
    resourceHandler.setResourceBase(webRootUri);

    HandlerCollection handlers = new HandlerCollection(context, resourceHandler);
    jettyServer.setHandler(handlers);

    try {
      ServerContainer container = WebSocketServerContainerInitializer.configureContext(context);
      container.addEndpoint(TerminalSocket.class);
      jettyServer.start();
      jettyServer.join();
    } catch (Exception e) {
      LOGGER.error(e.getMessage(), e);
    }
  }

  public boolean isRunning() {
    return jettyServer.isRunning();
  }

  public void stopRunning() {
    try {
      jettyServer.stop();
    } catch (Exception e) {
      LOGGER.error(e.getMessage(), e);
    }
    LOGGER.info("stop TerminalThread");
  }

  public void createTerminalWindow(InterpreterContext intpContext, String host, int port) {
    String url = "http://" + host + ":" + port;
    String terminalWindowTemplate = "<form style=\"width:100%\">\n" +
        "  <iframe src=\"" + url + "\"" +
        "    height=\"500\"" +
        "    width=\"100%\"" +
        "    frameborder=\"0\"" +
        "    scrolling=\"0\"" +
        "  ></iframe>\n" +
        "</form>";
    LOGGER.info(terminalWindowTemplate);
    try {
      intpContext.out.setType(InterpreterResult.Type.ANGULAR);
      InterpreterResultMessageOutput outputUI = intpContext.out.getOutputAt(0);
      outputUI.clear();
      outputUI.write(terminalWindowTemplate);
      outputUI.flush();
    } catch (IOException e) {
      LOGGER.error(e.getMessage(), e);
    }
  }
}
