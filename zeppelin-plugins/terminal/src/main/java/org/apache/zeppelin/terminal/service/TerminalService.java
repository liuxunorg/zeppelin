package org.apache.zeppelin.terminal.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.pty4j.PtyProcess;
import com.pty4j.WinSize;
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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.zeppelin.terminal.helper.IOHelper;
import org.apache.zeppelin.terminal.helper.ThreadHelper;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.LinkedBlockingQueue;

@Component
@Scope("prototype")
public class TerminalService {
  private static final Logger LOGGER = LoggerFactory.getLogger(TerminalService.class);

  @Value("${shell:#{null}}")
  private String shellStarter;

  private String[] termCommand;
  private PtyProcess process;
  private Integer columns = 20;
  private Integer rows = 10;
  private BufferedReader inputReader;
  private BufferedReader errorReader;
  private BufferedWriter outputWriter;
  private WebSocketSession webSocketSession;

  private LinkedBlockingQueue<String> commandQueue = new LinkedBlockingQueue<>();

  public void onTerminalInit() {

  }

  public void onTerminalReady() {
    ThreadHelper.start(() -> {
      try {
        initializeProcess();
      } catch (Exception e) {
        LOGGER.error(e.getMessage(), e);
      }
    });
  }

  private void initializeProcess() throws Exception {
    String userHome = System.getProperty("user.home");
    Path dataDir = Paths.get(userHome).resolve(".terminalfx");
    IOHelper.copyLibPty(dataDir);

    boolean isWindows = System.getProperty("os.name").startsWith("Windows");
    if ( isWindows) {
      this.termCommand = "cmd.exe".split("\\s+");
    } else {
      this.termCommand = "/bin/bash -i".split("\\s+");
    }

    if (null != shellStarter){
      this.termCommand = shellStarter.split("\\s+");
    }

    Map<String, String> envs = new HashMap<>(System.getenv());
    envs.put("TERM", "xterm");

    System.setProperty("PTY_LIB_FOLDER", dataDir.resolve("libpty").toString());

    this.process = PtyProcess.exec(termCommand, envs, userHome);

    process.setWinSize(new WinSize(columns, rows));
    this.inputReader = new BufferedReader(new InputStreamReader(process.getInputStream()));
    this.errorReader = new BufferedReader(new InputStreamReader(process.getErrorStream()));
    this.outputWriter = new BufferedWriter(new OutputStreamWriter(process.getOutputStream()));

    ThreadHelper.start(() -> {
      printReader(inputReader);
    });

    ThreadHelper.start(() -> {
      printReader(errorReader);
    });

    process.waitFor();
  }

  public void print(String text) throws IOException {

    Map<String, String> map = new HashMap<>();
    map.put("type", "TERMINAL_PRINT");
    map.put("text", text);

    String message = new ObjectMapper().writeValueAsString(map);

    webSocketSession.sendMessage(new TextMessage(message));
  }

  private void printReader(BufferedReader bufferedReader) {
    try {
      int nRead;
      char[] data = new char[1 * 1024];

      while ((nRead = bufferedReader.read(data, 0, data.length)) != -1) {
        StringBuilder builder = new StringBuilder(nRead);
        builder.append(data, 0, nRead);
        print(builder.toString());
      }
    } catch (Exception e) {
      LOGGER.error(e.getMessage(), e);
    }
  }

  public void onCommand(String command) throws InterruptedException {
    if (null == command || StringUtils.isEmpty(command)) {
      return;
    }

    commandQueue.put(command);
    ThreadHelper.start(() -> {
      try {
        outputWriter.write(commandQueue.poll());
        outputWriter.flush();
      } catch (IOException e) {
        LOGGER.error(e.getMessage(), e);
      }
    });
  }

  public void onTerminalResize(String columns, String rows) {
    if (Objects.nonNull(columns) && Objects.nonNull(rows)) {
      this.columns = Integer.valueOf(columns);
      this.rows = Integer.valueOf(rows);

      if (Objects.nonNull(process)) {
        process.setWinSize(new WinSize(this.columns, this.rows));
      }
    }
  }

  public void setWebSocketSession(WebSocketSession webSocketSession) {
    this.webSocketSession = webSocketSession;
  }

  public WebSocketSession getWebSocketSession() {
    return webSocketSession;
  }
}
