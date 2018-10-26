/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.zeppelin.submarine;
import org.apache.commons.exec.ExecuteException;
import org.apache.commons.exec.ExecuteWatchdog;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.PumpStreamHandler;
import org.apache.commons.exec.CommandLine;
import org.apache.commons.lang3.StringUtils;

import org.apache.hadoop.fs.Path;
import org.apache.zeppelin.submarine.utils.HDFSUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.zeppelin.interpreter.InterpreterContext;
import org.apache.zeppelin.interpreter.InterpreterException;
import org.apache.zeppelin.interpreter.InterpreterResult;
import org.apache.zeppelin.interpreter.thrift.InterpreterCompletion;
import org.apache.zeppelin.scheduler.Scheduler;
import org.apache.zeppelin.scheduler.SchedulerFactory;

/**
 * SubmarineTFInterpreter of Hadoop Submarine's tensorflow framework implementation.
 * Support for Hadoop Submarine cli. All the commands documented here
 * https://github.com/apache/hadoop/blob/trunk/hadoop-yarn-project/hadoop-yarn/
 * hadoop-yarn-applications/hadoop-yarn-submarine/src/site/markdown/QuickStart.md is supported.
 */
public class SubmarineTFInterpreter extends SubmarineInterpreter {
  private Logger LOGGER = LoggerFactory.getLogger(SubmarineTFInterpreter.class);

  public static final String ALGORITHM_UPLOAD_PATH    = "algorithm.upload.path";

  public static final String PARAMETER_SERVICES_DOCKER_IMAGE = "parameter.services.docker.image";
  public static final String PARAMETER_SERVICES_NUM = "parameter.services.num";
  public static final String PARAMETER_SERVICES_GPU = "parameter.services.gpu";
  public static final String PARAMETER_SERVICES_CPU = "parameter.services.cpu";
  public static final String PARAMETER_SERVICES_MEMORY = "parameter.services.memory";

  public static final String WORKER_SERVICES_DOCKER_IMAGE = "worker.services.docker.image";
  public static final String WORKER_SERVICES_NUM = "worker.services.num";
  public static final String WORKER_SERVICES_GPU = "worker.services.gpu";
  public static final String WORKER_SERVICES_CPU = "worker.services.gpu";
  public static final String WORKER_SERVICES_MEMORY = "worker.services.memory";

  public static final String TENSORBOARD_ENABLE  = "tensorboard.enable";

  private static final String DIRECTORY_USER_HOME = "shell.working.directory.user.home";
  private final boolean isWindows = System.getProperty("os.name").startsWith("Windows");
  private final String shell = isWindows ? "cmd /c" : "bash -c";

  private static final String TIMEOUT_PROPERTY = "submarine.command.timeout.millisecond";
  private String defaultTimeoutProperty = "60000";
  ConcurrentHashMap<String, DefaultExecutor> executors;

  LinkedBlockingQueue<SubmarineParagraph> executorQueue = null;
  // SubmarineParagraph submarineParagraph = null;

  String algorithmUploadPath = "";
  HDFSUtils hdfsUtils = null;

  public SubmarineTFInterpreter(Properties property) {
    super(property);

    algorithmUploadPath = this.getProperty(ALGORITHM_UPLOAD_PATH);
    hdfsUtils = new HDFSUtils(algorithmUploadPath);
  }

  @Override
  public void open() {
    super.open();
    LOGGER.info("Command timeout property: {}", getProperty(TIMEOUT_PROPERTY));
    executors = new ConcurrentHashMap<>();
    executorQueue = new LinkedBlockingQueue(2);
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
  public InterpreterResult interpret(String originalCmd, InterpreterContext contextInterpreter) {
    SubmarineParagraph submarineParagraph = new SubmarineParagraph(
        contextInterpreter.getNoteId(),
        contextInterpreter.getNoteName(),
        contextInterpreter.getParagraphId(),
        contextInterpreter.getParagraphTitle(),
        contextInterpreter.getParagraphText(),
        contextInterpreter.getReplName());

    // upload algorithm to HDFS
    try {
      uploadAlgorithmToHDFS(submarineParagraph);
    } catch (Exception e) {
      e.printStackTrace();
      return new InterpreterResult(InterpreterResult.Code.ERROR, e.getMessage());
    }

    String cmd = Boolean.parseBoolean(getProperty("zeppelin.shell.interpolation")) ?
        interpolate(originalCmd, contextInterpreter.getResourcePool()) : originalCmd;
    LOGGER.info("Run shell command '" + cmd + "'");
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
          contextInterpreter.out, contextInterpreter.out));

      try {
        Thread.sleep(20000);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }

      executor.setWatchdog(new ExecuteWatchdog(
          Long.valueOf(getProperty(TIMEOUT_PROPERTY, defaultTimeoutProperty))));
      executors.put(contextInterpreter.getParagraphId(), executor);
      if (Boolean.valueOf(getProperty(DIRECTORY_USER_HOME))) {
        executor.setWorkingDirectory(new File(System.getProperty("user.home")));
      }

      int exitVal = executor.execute(cmdLine);
      LOGGER.info("Paragraph " + contextInterpreter.getParagraphId()
          + " return with exit value: " + exitVal);
      return new InterpreterResult(InterpreterResult.Code.SUCCESS, outStream.toString());
    } catch (ExecuteException e) {
      int exitValue = e.getExitValue();
      LOGGER.error("Can not run " + cmd, e);
      InterpreterResult.Code code = InterpreterResult.Code.ERROR;
      String message = outStream.toString();
      if (exitValue == 143) {
        code = InterpreterResult.Code.INCOMPLETE;
        message += "Paragraph received a SIGTERM\n";
        LOGGER.info("The paragraph " + contextInterpreter.getParagraphId()
            + " stopped executing: " + message);
      }
      message += "ExitValue: " + exitValue;
      return new InterpreterResult(code, message);
    } catch (IOException e) {
      LOGGER.error("Can not run " + cmd, e);
      return new InterpreterResult(InterpreterResult.Code.ERROR, e.getMessage());
    } finally {
      executors.remove(contextInterpreter.getParagraphId());
    }
  }

  @Override
  public void cancel(InterpreterContext context) {
    removeParagraph(context.getNoteId(), context.getParagraphId());

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
    String schedulerName = SubmarineTFInterpreter.class.getName() + this.hashCode();
    if (concurrentExecutedMax > 1) {
      return SchedulerFactory.singleton().createOrGetParallelScheduler(schedulerName,
          concurrentExecutedMax);
    } else {
      return SchedulerFactory.singleton().createOrGetFIFOScheduler(schedulerName);
    }
  }

  @Override
  public List<InterpreterCompletion> completion(
      String buf, int cursor, InterpreterContext interpreterContext) {
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
        properties.getProperty("zeppelin.shell.keytab.location"),
        properties.getProperty("zeppelin.shell.principal"));
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
    /*
    if (!StringUtils.isAnyEmpty(getProperty("zeppelin.shell.auth.type")) && getProperty(
        "zeppelin.shell.auth.type").equalsIgnoreCase("kerberos")) {
      return true;
    }*/
    return false;
  }

  private void uploadAlgorithmToHDFS(SubmarineParagraph paragraph) throws Exception {
    String paragraphDir = algorithmUploadPath + File.pathSeparator + paragraph.getNoteId()
        + File.pathSeparator + paragraph.getParagraphId();
    Path paragraphPath = new Path(paragraphDir);
    if (hdfsUtils.exists(paragraphPath)) {
      hdfsUtils.tryMkDir(paragraphPath);
    }

    LOGGER.info("Upload algorithm to HDFS: {}", paragraphDir);
    Path algorithmPath = new Path(paragraphDir + File.pathSeparator + "algorithm.python");
    hdfsUtils.writeFile(paragraph.getParagraphText(), algorithmPath);
  }

  //
  private void removeParagraph(String noteId, String paragraphId) {
    Iterator iter = executorQueue.iterator();
    while (iter.hasNext()) {
      SubmarineParagraph paragraph = (SubmarineParagraph) iter.next();
      if (paragraph.getNoteId().equals(noteId) && paragraph.getParagraphId().equals(paragraphId)) {
        executorQueue.remove(paragraph);
      }
    }
  }
}
