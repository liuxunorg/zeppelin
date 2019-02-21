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

package org.apache.zeppelin.submarine;

import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.ExecuteException;
import org.apache.commons.exec.ExecuteWatchdog;
import org.apache.commons.exec.PumpStreamHandler;
import org.apache.commons.lang3.StringUtils;
import org.apache.zeppelin.interpreter.BaseZeppelinContext;
import org.apache.zeppelin.interpreter.InterpreterContext;
import org.apache.zeppelin.interpreter.InterpreterException;
import org.apache.zeppelin.interpreter.InterpreterResult;
import org.apache.zeppelin.interpreter.InterpreterResult.Code;
import org.apache.zeppelin.interpreter.KerberosInterpreter;
import org.apache.zeppelin.interpreter.thrift.InterpreterCompletion;
import org.apache.zeppelin.scheduler.Scheduler;
import org.apache.zeppelin.scheduler.SchedulerFactory;
import org.apache.zeppelin.submarine.componts.SubmarineConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Submarine Shell interpreter for Zeppelin.
 */
public class SubmarineShellInterpreter extends KerberosInterpreter {
  private static final Logger LOGGER = LoggerFactory.getLogger(SubmarineShellInterpreter.class);

  private static final String TIMEOUT_PROPERTY = "shell.command.timeout.millisecs";
  private String defaultTimeoutProperty = "60000";

  private static final String DIRECTORY_USER_HOME = "shell.working.directory.user.home";
  private final boolean isWindows = System.getProperty("os.name").startsWith("Windows");
  private final String shell = isWindows ? "cmd /c" : "bash -c";
  ConcurrentHashMap<String, DefaultExecutor> executors;

  public SubmarineShellInterpreter(Properties property) {
    super(property);
  }

  @Override
  public void open() {
    super.open();
    LOGGER.info("Command timeout property: {}", getProperty(TIMEOUT_PROPERTY));
    executors = new ConcurrentHashMap<>();
  }

  @Override
  public void close() {
    super.close();
    for (String executorKey : executors.keySet()) {
      DefaultExecutor executor = executors.remove(executorKey);
      if (executor != null) {
        try {
          executor.getWatchdog().destroyProcess();
        } catch (Exception e){
          LOGGER.error("error destroying executor for paragraphId: " + executorKey, e);
        }
      }
    }
  }

  @Override
  public InterpreterResult internalInterpret(String originalCmd, InterpreterContext intpContext) {
    // algorithm & checkpoint path support replaces ${username} with real user name
    String algorithmPath = properties.getProperty(
        SubmarineConstants.SUBMARINE_ALGORITHM_HDFS_PATH, "");
    if (algorithmPath.contains(SubmarineConstants.USERNAME_SYMBOL)) {
      algorithmPath = algorithmPath.replace(SubmarineConstants.USERNAME_SYMBOL, userName);
      properties.setProperty(SubmarineConstants.SUBMARINE_ALGORITHM_HDFS_PATH, algorithmPath);
    }
    String checkpointPath = properties.getProperty(
        SubmarineConstants.TF_CHECKPOINT_PATH, "");
    if (checkpointPath.contains(SubmarineConstants.USERNAME_SYMBOL)) {
      checkpointPath = checkpointPath.replace(SubmarineConstants.USERNAME_SYMBOL, userName);
      properties.setProperty(SubmarineConstants.TF_CHECKPOINT_PATH, checkpointPath);
    }

    String cmd = Boolean.parseBoolean(getProperty("zeppelin.shell.interpolation")) ?
            interpolate(originalCmd, intpContext.getResourcePool()) : originalCmd;
    LOGGER.debug("Run shell command '" + cmd + "'");
    OutputStream outStream = new ByteArrayOutputStream();
    
    CommandLine cmdLine = CommandLine.parse(shell);
    // the Windows CMD shell doesn't handle multiline statements,
    // they need to be delimited by '&&' instead
    if (isWindows) {
      String[] lines = StringUtils.split(cmd, "\n");
      cmd = StringUtils.join(lines, " && ");
    }
    cmdLine.addArgument(cmd, false);

    try {
      DefaultExecutor executor = new DefaultExecutor();
      executor.setStreamHandler(new PumpStreamHandler(
          intpContext.out, intpContext.out));

      executor.setWatchdog(new ExecuteWatchdog(
          Long.valueOf(getProperty(TIMEOUT_PROPERTY, defaultTimeoutProperty))));
      executors.put(intpContext.getParagraphId(), executor);
      if (Boolean.valueOf(getProperty(DIRECTORY_USER_HOME))) {
        executor.setWorkingDirectory(new File(System.getProperty("user.home")));
      }

      int exitVal = executor.execute(cmdLine);
      LOGGER.info("Paragraph " + intpContext.getParagraphId()
          + " return with exit value: " + exitVal);
      return new InterpreterResult(Code.SUCCESS, outStream.toString());
    } catch (ExecuteException e) {
      int exitValue = e.getExitValue();
      LOGGER.error("Can not run " + cmd, e);
      Code code = Code.ERROR;
      String message = outStream.toString();
      if (exitValue == 143) {
        code = Code.INCOMPLETE;
        message += "Paragraph received a SIGTERM\n";
        LOGGER.info("The paragraph " + intpContext.getParagraphId()
            + " stopped executing: " + message);
      }
      message += "ExitValue: " + exitValue;
      return new InterpreterResult(code, message);
    } catch (IOException e) {
      LOGGER.error("Can not run " + cmd, e);
      return new InterpreterResult(Code.ERROR, e.getMessage());
    } finally {
      executors.remove(intpContext.getParagraphId());
    }
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
  public void cancel(InterpreterContext context) {
    DefaultExecutor executor = executors.remove(context.getParagraphId());
    if (executor != null) {
      try {
        executor.getWatchdog().destroyProcess();
      } catch (Exception e){
        LOGGER.error("error destroying executor for paragraphId: " + context.getParagraphId(), e);
      }
    }
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
        SubmarineShellInterpreter.class.getName() + this.hashCode(), 10);
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
    Properties properties = getProperties();
    CommandLine cmdLine = CommandLine.parse(shell);
    cmdLine.addArgument("-c", false);
    String kinitCommand = String.format("kinit -k -t %s %s",
        properties.getProperty(SubmarineConstants.SUBMARINE_HADOOP_PRINCIPAL),
        properties.getProperty(SubmarineConstants.SUBMARINE_HADOOP_KEYTAB));
    cmdLine.addArgument(kinitCommand, false);
    DefaultExecutor executor = new DefaultExecutor();
    try {
      executor.execute(cmdLine);
    } catch (Exception e) {
      LOGGER.error("Unable to run kinit for zeppelin user " + kinitCommand, e);
      throw new InterpreterException(e);
    }
  }

  @Override
  protected boolean isKerboseEnabled() {
    if (!StringUtils.isAnyEmpty(getProperty(SubmarineConstants.ZEPPELIN_SUBMARINE_AUTH_TYPE))
        && getProperty(SubmarineConstants.ZEPPELIN_SUBMARINE_AUTH_TYPE)
        .equalsIgnoreCase("kerberos")) {
      return true;
    }
    return false;
  }

}
