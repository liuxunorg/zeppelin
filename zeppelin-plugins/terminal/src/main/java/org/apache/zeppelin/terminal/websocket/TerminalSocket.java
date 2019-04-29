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

package org.apache.zeppelin.terminal.websocket;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.zeppelin.terminal.service.TerminalService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.TextWebSocketHandler;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class TerminalSocket extends TextWebSocketHandler {
  private static final Logger LOGGER = LoggerFactory.getLogger(TerminalSocket.class);
  private final TerminalService terminalService;

  @Autowired
  public TerminalSocket(TerminalService terminalService) {
    this.terminalService = terminalService;
  }

  @Override
  public void afterConnectionEstablished(WebSocketSession session) throws Exception {
    terminalService.setWebSocketSession(session);
    super.afterConnectionEstablished(session);
  }

  @Override
  protected void handleTextMessage(WebSocketSession session, TextMessage message) throws Exception {
    Map<String, String> messageMap = getMessageMap(message);

    if (messageMap.containsKey("type")) {
      String type = messageMap.get("type");

      switch (type) {
        case "TERMINAL_INIT":
          terminalService.onTerminalInit();
          break;
        case "TERMINAL_READY":
          terminalService.onTerminalReady();
          break;
        case "TERMINAL_COMMAND":
          terminalService.onCommand(messageMap.get("command"));
          break;
        case "TERMINAL_RESIZE":
          terminalService.onTerminalResize(messageMap.get("columns"), messageMap.get("rows"));
          break;
        default:
          throw new RuntimeException("Unrecodnized action");
      }
    }
  }

  private Map<String, String> getMessageMap(TextMessage message) {
    try {
      Map<String, String> map = new ObjectMapper().readValue(message.getPayload(),
          new TypeReference<Map<String, String>>() {});

      return map;
    } catch (IOException e) {
      LOGGER.error(e.getMessage(), e);
    }
    return new HashMap<>();
  }

  @Override
  public void handleTransportError(WebSocketSession session, Throwable exception) throws Exception {
    super.handleTransportError(session, exception);
  }

  @Override
  public void afterConnectionClosed(WebSocketSession session, CloseStatus status) throws Exception {
    super.afterConnectionClosed(session, status);
  }

  @Override
  public boolean supportsPartialMessages() {
    return super.supportsPartialMessages();
  }
}
