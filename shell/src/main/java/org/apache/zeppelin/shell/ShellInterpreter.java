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

import org.apache.commons.lang3.StringUtils;
import org.apache.zeppelin.interpreter.BaseZeppelinContext;
import org.apache.zeppelin.interpreter.remote.RemoteInterpreterUtils;
import org.apache.zeppelin.terminal.TerminalThread;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Properties;

import org.apache.zeppelin.interpreter.InterpreterContext;
import org.apache.zeppelin.interpreter.InterpreterException;
import org.apache.zeppelin.interpreter.InterpreterResult;
import org.apache.zeppelin.interpreter.InterpreterResult.Code;
import org.apache.zeppelin.interpreter.KerberosInterpreter;
import org.apache.zeppelin.interpreter.thrift.InterpreterCompletion;
import org.apache.zeppelin.scheduler.Scheduler;
import org.apache.zeppelin.scheduler.SchedulerFactory;

/**
 * Shell interpreter for Zeppelin.
 */
public class ShellInterpreter extends KerberosInterpreter {
  private static final Logger LOGGER = LoggerFactory.getLogger(ShellInterpreter.class);

  public ShellInterpreter(Properties property) {
    super(property);
  }

  private TerminalThread terminalThread = null;

  @Override
  public void open() {
    super.open();
  }

  private void setParagraphConfig(InterpreterContext intpContext) {
    intpContext.getConfig().put("editorHide", true);
    intpContext.getConfig().put("title", false);
  }

  @Override
  public void close() {
    if (null != terminalThread) {
      terminalThread.stopRunning();
    }
    super.close();
  }

  @Override
  protected boolean isInterpolate() {
    return Boolean.parseBoolean(getProperty("zeppelin.shell.interpolation", "false"));
  }

  @Override
  public BaseZeppelinContext getZeppelinContext() {
    return null;
  }

  @Override
  public InterpreterResult internalInterpret(String cmd, InterpreterContext intpContext) {
    if (null == terminalThread) {
      String host = "";
      int port = 0;
      try {
        host = RemoteInterpreterUtils.findAvailableHostAddress();
        port = RemoteInterpreterUtils.findRandomAvailablePortOnAllLocalInterfaces();
        terminalThread = new TerminalThread(port);
        terminalThread.start();
      } catch (IOException e) {
        LOGGER.error(e.getMessage(), e);
        return new InterpreterResult(Code.ERROR, e.getMessage());
      }

      for (int i = 0; i < 20 && !terminalThread.isRunning(); i++) {
        try {
          LOGGER.info("loop = " + i);
          Thread.sleep(500);
        } catch (InterruptedException e) {
          LOGGER.error(e.getMessage(), e);
        }
      }
      setParagraphConfig(intpContext);
      terminalThread.createTerminalWindow(intpContext, host, port);
    }

    return new InterpreterResult(Code.SUCCESS);
  }

  @Override
  public void cancel(InterpreterContext context) {
  }

  @Override
  public FormType getFormType() {
    return FormType.SIMPLE;
  }

  @Override
  public int getProgress(InterpreterContext context) {
    return 0;
  }

  @Override
  public Scheduler getScheduler() {
    return SchedulerFactory.singleton().createOrGetParallelScheduler(
        ShellInterpreter.class.getName() + this.hashCode(), 10);
  }

  @Override
  public List<InterpreterCompletion> completion(String buf, int cursor,
      InterpreterContext interpreterContext) {
    return null;
  }

  @Override
  protected boolean runKerberosLogin() {
    try {
      createSecureConfiguration();
      return true;
    } catch (Exception e) {
      LOGGER.error("Unable to run kinit for zeppelin", e);
    }
    return false;
  }

  public void createSecureConfiguration() throws InterpreterException {
    String kinitCommand = String.format("kinit -k -t %s %s",
        properties.getProperty("zeppelin.shell.keytab.location"),
        properties.getProperty("zeppelin.shell.principal"));
  }

  @Override
  protected boolean isKerboseEnabled() {
    if (!StringUtils.isAnyEmpty(getProperty("zeppelin.shell.auth.type")) && getProperty(
        "zeppelin.shell.auth.type").equalsIgnoreCase("kerberos")) {
      return true;
    }
    return false;
  }

  public static void startThread(Runnable runnable) {
    Thread thread = new Thread(runnable);
    thread.start();
  }
}
