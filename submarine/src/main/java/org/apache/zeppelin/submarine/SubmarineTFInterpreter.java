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
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.CommandLine;
import org.apache.commons.lang3.StringUtils;

import org.apache.hadoop.fs.Path;
import org.apache.zeppelin.interpreter.KerberosInterpreter;
import org.apache.zeppelin.submarine.utils.SubmarineConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.List;
import java.util.Properties;

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
public class SubmarineTFInterpreter extends KerberosInterpreter {
  private Logger LOGGER = LoggerFactory.getLogger(SubmarineTFInterpreter.class);

  private final boolean isWindows = System.getProperty("os.name").startsWith("Windows");
  private final String shell = isWindows ? "cmd /c" : "bash -c";

  private static final String TIMEOUT_PROPERTY = "submarine.command.timeout.millisecond";
  private String defaultTimeoutProperty = "60000";

  private SubmarineContext submarineContext = null;

  // Number of submarines executed in parallel for each interpreter instance
  protected int concurrentExecutedMax = 1;

  public SubmarineTFInterpreter(Properties property) {
    super(property);

    submarineContext = SubmarineContext.getInstance(properties);
  }

  @Override
  public void open() {
    super.open();
    LOGGER.info("Command timeout property: {}", getProperty(TIMEOUT_PROPERTY));
  }

  @Override
  public void close() {
    super.close();
  }

  @Override
  public InterpreterResult interpret(String script, InterpreterContext contextIntp) {
    String fileName = contextIntp.getParagraphTitle();
    if (null == fileName || StringUtils.isEmpty(fileName)) {
      return new InterpreterResult(InterpreterResult.Code.ERROR,
          "ERROR: Please set this paragraph title!");
    }
    try {
      // upload algorithm to HDFS
      String hdfsFile = uploadAlgorithmToHDFS(contextIntp.getNoteId(), fileName, script);

      String message = "Commit " + fileName + " to HDFS " + hdfsFile + " success!";
      LOGGER.info(message);
      return new InterpreterResult(InterpreterResult.Code.SUCCESS, message);
    } catch (Exception e) {
      LOGGER.error("uploadAlgorithmToHDFS ERROR : ", e);
      return new InterpreterResult(InterpreterResult.Code.ERROR, e.getMessage());
    } finally {
    }
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

  private String uploadAlgorithmToHDFS(String noteId, String fileName, String script)
      throws Exception {
    String algorithmUploadPath = getProperty(
        SubmarineConstants.SUBMARINE_ALGORITHM_HDFS_PATH, "");
    if (StringUtils.isEmpty(algorithmUploadPath)) {
      String msg = "Please set the submarine interpreter properties : "
          + SubmarineConstants.SUBMARINE_ALGORITHM_HDFS_PATH + "\n";
      throw new RuntimeException(msg);
    }

    String uploadDir = algorithmUploadPath + File.separator + noteId;
    String fileDir = uploadDir + File.separator + fileName;

    try {
      // create file dir
      Path uploadPath = new Path(uploadDir);
      if (!submarineContext.getHDFSUtils().exists(uploadPath)) {
        submarineContext.getHDFSUtils().tryMkDir(uploadPath);
      }

      // upload algorithm file
      LOGGER.info("Commit algorithm to HDFS: {}", fileDir);
      Path filePath = new Path(fileDir);
      submarineContext.getHDFSUtils().writeFile(script, filePath);
    } catch (Exception e) {
      throw new RuntimeException("Commit algorithm to HDFS failure!", e);
    }

    return fileDir;
  }
}
