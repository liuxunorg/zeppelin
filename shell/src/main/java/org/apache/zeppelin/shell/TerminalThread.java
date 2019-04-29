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

package org.apache.zeppelin.shell;

import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecuteResultHandler;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.ExecuteException;
import org.apache.commons.exec.ExecuteWatchdog;
import org.apache.commons.exec.LogOutputStream;
import org.apache.commons.exec.PumpStreamHandler;
import org.apache.zeppelin.interpreter.InterpreterContext;
import org.apache.zeppelin.interpreter.InterpreterResult;
import org.apache.zeppelin.interpreter.InterpreterResultMessageOutput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public class TerminalThread extends Thread {
  private static final Logger LOGGER = LoggerFactory.getLogger(TerminalThread.class);

  private static final boolean isWindows = System.getProperty("os.name").startsWith("Windows");
  public static final String shell = isWindows ? "cmd /c" : "bash -c";

  DefaultExecutor executor = new DefaultExecutor();
  ExecuteWatchdog watchDog = new ExecuteWatchdog(Integer.MAX_VALUE);
  private AtomicBoolean waitStopRunning = new AtomicBoolean(false);

  private AtomicBoolean running = new AtomicBoolean(false);

  private int port = 0;
  public TerminalThread(int port) {
    this.port = port;
  }

  public void run() {
    try {
      String cmd = "/home/hadoop/java-current/bin/java " +
          "-Dlog4j.configuration=file:///home/hadoop/zeppelin-current/conf/log4j.properties " +
          "-jar " +
          "/home/hadoop/zeppelin-current/plugins/" +
          "terminal/zeppelin-terminal-0.9.0-SNAPSHOT.jar --server.port=" + port;
      CommandLine cmdLine = CommandLine.parse(shell);
      cmdLine.addArgument(cmd, false);
      executor.setWatchdog(watchDog);

      executor.setStreamHandler(new PumpStreamHandler(new LogOutputStream() {
        @Override
        protected void processLine(String line, int level) {
          if (null != line) {
            LOGGER.info(line);
            if (line.contains("Started TerminalApp in")) {
              LOGGER.info("Started TerminalApp!");
              running.set(true);
            }
          }
        }
      }));

      Map<String, String> env = new HashMap<>();
      executor.execute(cmdLine, env, new DefaultExecuteResultHandler() {
        @Override
        public void onProcessComplete(int exitValue) {
          LOGGER.info("Complete");
        }

        @Override
        public void onProcessFailed(ExecuteException e) {
          String message = String.format(
              "onProcessFailed exit value is : %d, exception is : %s",
              e.getExitValue(), e.getMessage());
          LOGGER.error(message);
          waitStopRunning.notify();
        }
      });
      waitStopRunning.wait();
    } catch (InterruptedException e) {
      LOGGER.error(e.getMessage(), e);
    } catch (IOException e) {
      LOGGER.error(e.getMessage(), e);
    }
  }

  public boolean isRunning() {
    return running.get();
  }

  public void stopRunning() {
    LOGGER.info("stopRunning ...");
    waitStopRunning.notify();

    if (watchDog.isWatching()) {
      watchDog.destroyProcess();
    }
    if (watchDog.isWatching()) {
      watchDog.killedProcess();
    }
    LOGGER.info("stopRunning");
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
