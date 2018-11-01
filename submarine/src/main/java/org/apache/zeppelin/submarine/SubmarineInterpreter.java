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

import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import com.hubspot.jinjava.Jinjava;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.ExecuteException;
import org.apache.commons.exec.ExecuteWatchdog;
import org.apache.commons.exec.PumpStreamHandler;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.zeppelin.interpreter.KerberosInterpreter;
import org.apache.zeppelin.interpreter.InterpreterContext;
import org.apache.zeppelin.interpreter.InterpreterResult;
import org.apache.zeppelin.interpreter.InterpreterException;
import org.apache.zeppelin.interpreter.InterpreterOutput;
import org.apache.zeppelin.interpreter.thrift.InterpreterCompletion;
import org.apache.zeppelin.scheduler.Scheduler;
import org.apache.zeppelin.scheduler.SchedulerFactory;
import org.apache.zeppelin.submarine.utils.CommandParser;
import org.apache.zeppelin.submarine.utils.SubmarineConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URL;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;

/**
 * SubmarineInterpreter of Hadoop Submarine implementation.
 * Support for Hadoop Submarine cli. All the commands documented here
 * https://github.com/apache/hadoop/blob/trunk/hadoop-yarn-project/hadoop-yarn/
 * hadoop-yarn-applications/hadoop-yarn-submarine/src/site/markdown/QuickStart.md is supported.
 */
public class SubmarineInterpreter extends KerberosInterpreter {
  private Logger LOGGER = LoggerFactory.getLogger(SubmarineInterpreter.class);

  private String submarineJobRunTFJinja = "submarine-job-run-tf.jinja";

  // Number of submarines executed in parallel for each interpreter instance
  protected int concurrentExecutedMax = 1;

  private static final String DIRECTORY_USER_HOME = "shell.working.directory.user.home";
  private final boolean isWindows = System.getProperty("os.name").startsWith("Windows");
  private final String shell = isWindows ? "cmd /c" : "bash -c";

  private static final String TIMEOUT_PROPERTY = "submarine.command.timeout.millisecond";
  private String defaultTimeoutProperty = "60000";

  private String hadoopHome;
  private String submarineJar;

  private SubmarineContext submarineContext = null;

  public SubmarineInterpreter(Properties property) {
    super(property);

    submarineContext = SubmarineContext.getInstance(properties);

    concurrentExecutedMax = Integer.parseInt(
        getProperty(SubmarineConstants.SUBMARINE_CONCURRENT_MAX, "1"));

    hadoopHome = properties.getProperty(SubmarineConstants.HADOOP_HOME, "");
    if (StringUtils.isEmpty(hadoopHome)) {
      LOGGER.error("Please set the submarine interpreter properties : " +
          SubmarineConstants.HADOOP_HOME);
    }
    File file = new File(hadoopHome);
    if (!file.exists()) {
      LOGGER.error(hadoopHome + " is not a valid file path!");
    }

    submarineJar = properties.getProperty(SubmarineConstants.HADOOP_YARN_SUBMARINE_JAR, "");
    if (StringUtils.isEmpty(submarineJar)) {
      LOGGER.error("Please set the submarine interpreter properties : " +
          SubmarineConstants.HADOOP_YARN_SUBMARINE_JAR);
    }
    File file2 = new File(submarineJar);
    if (!file2.exists()) {
      LOGGER.error(submarineJar + " is not a valid file path!");
    }
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
    LOGGER.debug("Run shell command '" + script + "'");
    OutputStream outStream = new ByteArrayOutputStream();
    String jobName = getJobName(contextIntp);

    try {
      CommandParser commandParser = new CommandParser();
      commandParser.populate(script);

      Properties properties = submarineContext.getProperties(jobName);
      properties.put(SubmarineConstants.JOB_NAME, jobName);
      properties.put(SubmarineConstants.NOTE_ID, contextIntp.getNoteId());
      properties.put(SubmarineConstants.NOTE_NAME, contextIntp.getNoteName());
      properties.put(SubmarineConstants.PARAGRAPH_ID, contextIntp.getParagraphId());
      properties.put(SubmarineConstants.PARAGRAPH_TEXT, contextIntp.getParagraphText());
      properties.put(SubmarineConstants.REPL_NAME, contextIntp.getReplName());
      properties.put(SubmarineConstants.SCRIPT, script);

      // Get the set variables from the user's execution script
      String inputPath = commandParser.getConfig(SubmarineConstants.INPUT_PATH, "");
      String checkPntPath = commandParser.getConfig(SubmarineConstants.CHECKPOINT_PATH, "");
      String psLaunchCmd = commandParser.getConfig(SubmarineConstants.PS_LAUNCH_CMD, "");
      String workerLaunchCmd = commandParser.getConfig(SubmarineConstants.WORKER_LAUNCH_CMD, "");
      properties.put(SubmarineConstants.INPUT_PATH, inputPath != null ? inputPath : "");
      properties.put(SubmarineConstants.CHECKPOINT_PATH, checkPntPath != null ? checkPntPath : "");
      properties.put(SubmarineConstants.PS_LAUNCH_CMD, psLaunchCmd != null ? psLaunchCmd : "");
      properties.put(SubmarineConstants.WORKER_LAUNCH_CMD,
          workerLaunchCmd != null ? workerLaunchCmd : "");

      String command = commandParser.getCommand();

      CommandLine cmdLine = CommandLine.parse(shell);
      cmdLine.addArgument(script, false);

      if (command.equalsIgnoreCase(SubmarineConstants.HELP_COMMAND)) {
        String message = getSubmarineHelp();
        return new InterpreterResult(InterpreterResult.Code.SUCCESS, message);
      } else if (command.equalsIgnoreCase(SubmarineConstants.COMMAND_JOB_SHOW)) {
        return jobShow(jobName, contextIntp.out, outStream);
      } else if (command.equals(SubmarineConstants.COMMAND_JOB_RUN)) {
        return jobRun(jobName, contextIntp.out, outStream);
      } else {
        String message = "ERROR: Unsupported command [" + command + "] !";
        message += getSubmarineHelp();
        return new InterpreterResult(InterpreterResult.Code.ERROR, message);
      }
    } catch (ExecuteException e) {
      int exitValue = e.getExitValue();
      LOGGER.error("Can not run " + script, e);
      InterpreterResult.Code code = InterpreterResult.Code.ERROR;
      String message = outStream.toString();
      if (exitValue == 143) {
        code = InterpreterResult.Code.INCOMPLETE;
        message += "Paragraph received a SIGTERM\n";
        LOGGER.info("The paragraph " + contextIntp.getParagraphId()
            + " stopped executing: " + message);
      }
      message += "ExitValue: " + exitValue;

      return new InterpreterResult(code, message);
    } catch (Exception e) {
      LOGGER.error("Can not run " + script, e);
      return new InterpreterResult(InterpreterResult.Code.ERROR, e.getMessage());
    } finally {

    }
  }

  @Override
  public void cancel(InterpreterContext context) {
    String jobName = getJobName(context);
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
        SubmarineInterpreter.class.getName() + this.hashCode(), 10);
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

  private String getJobName(InterpreterContext contextIntp) {
    return contextIntp.getNoteId();
  }

  private InterpreterResult jobRun(String jobName, InterpreterOutput output, OutputStream outStream)
      throws IOException {
    HashMap jinjaParams = propertiesToJinjaParams(jobName);

    URL urlTemplate = Resources.getResource(submarineJobRunTFJinja);
    String template = Resources.toString(urlTemplate, Charsets.UTF_8);

    Jinjava jinjava = new Jinjava();
    String submarineCmd = jinjava.render(template, jinjaParams);

    LOGGER.info("Execute : " + submarineCmd);
    output.write("Execute : " + submarineCmd);

    String cmd = "echo > " + submarineCmd;

    CommandLine cmdLine = CommandLine.parse(shell);
    cmdLine.addArgument(cmd, false);

    DefaultExecutor executor = new DefaultExecutor();
    executor.setStreamHandler(new PumpStreamHandler(output, output));

    long timeout = Long.valueOf(getProperty(TIMEOUT_PROPERTY, defaultTimeoutProperty));

    executor.setWatchdog(new ExecuteWatchdog(timeout));
    if (Boolean.valueOf(getProperty(DIRECTORY_USER_HOME))) {
      executor.setWorkingDirectory(new File(System.getProperty("user.home")));
    }

    int exitVal = executor.execute(cmdLine);
    LOGGER.info("jobName {} return with exit value: {}", jobName, exitVal);

    return new InterpreterResult(InterpreterResult.Code.SUCCESS, outStream.toString());
  }

  private StringBuffer formatUserProperties(StringBuffer sbMessage, String key, String info) {
    sbMessage.append("Please set the parameter ");
    sbMessage.append(key);
    sbMessage.append(" first, \nfor example: ");
    sbMessage.append(key + info);

    return sbMessage;
  }

  private void setInptPropertiesWarn(StringBuffer sbMessage, String key) {
    sbMessage.append("Please set the submarine interpreter properties : ");
    sbMessage.append(key).append("\n");
  }

  // Convert properties to Map and check that the variable cannot be empty
  private HashMap propertiesToJinjaParams(String noteId)
      throws IOException {
    StringBuffer sbMessage = new StringBuffer();

    String inputPath = submarineContext.getPropertie(noteId,
        SubmarineConstants.INPUT_PATH);
    if (StringUtils.isEmpty(inputPath)) {
      formatUserProperties(sbMessage, SubmarineConstants.INPUT_PATH, "=path...\n");
    }
    String checkPointPath = submarineContext.getPropertie(noteId,
        SubmarineConstants.CHECKPOINT_PATH);
    if (StringUtils.isEmpty(checkPointPath)) {
      formatUserProperties(sbMessage, SubmarineConstants.CHECKPOINT_PATH, "=path...\n");
    }
    String psLaunchCmd = submarineContext.getPropertie(noteId,
        SubmarineConstants.PS_LAUNCH_CMD);
    if (StringUtils.isEmpty(psLaunchCmd)) {
      formatUserProperties(sbMessage, SubmarineConstants.PS_LAUNCH_CMD,
          "=\"python /test/cifar10_estimator/cifar10_main.py " +
          "--data-dir=hdfs://mldev/tmp/cifar-10-data " +
          "--job-dir=hdfs://mldev/tmp/cifar-10-jobdir --num-gpus=0\"\n");
    }
    String workerLaunchCmd = submarineContext.getPropertie(noteId,
        SubmarineConstants.WORKER_LAUNCH_CMD);
    if (StringUtils.isEmpty(workerLaunchCmd)) {
      formatUserProperties(sbMessage, SubmarineConstants.WORKER_LAUNCH_CMD,
          "=\"python /test/cifar10_estimator/cifar10_main.py " +
          "--data-dir=hdfs://mldev/tmp/cifar-10-data " +
          "--job-dir=hdfs://mldev/tmp/cifar-10-jobdir " +
          "--train-steps=500 --eval-batch-size=16 --train-batch-size=16 --sync --num-gpus=1\"\n");
    }

    String containerNetwork = properties.getProperty(
        SubmarineConstants.DOCKER_CONTAINER_NETWORK, "");
    if (StringUtils.isEmpty(containerNetwork)) {
      setInptPropertiesWarn(sbMessage, SubmarineConstants.DOCKER_CONTAINER_NETWORK);
    }
    String parameterServicesImage = properties.getProperty(
        SubmarineConstants.PARAMETER_SERVICES_DOCKER_IMAGE, "");
    if (StringUtils.isEmpty(parameterServicesImage)) {
      setInptPropertiesWarn(sbMessage, SubmarineConstants.PARAMETER_SERVICES_DOCKER_IMAGE);
    }
    String parameterServicesNum = properties.getProperty(
        SubmarineConstants.PARAMETER_SERVICES_NUM, "");
    if (StringUtils.isEmpty(parameterServicesNum)) {
      setInptPropertiesWarn(sbMessage, SubmarineConstants.PARAMETER_SERVICES_NUM);
    }
    String parameterServicesGpu = properties.getProperty(
        SubmarineConstants.PARAMETER_SERVICES_GPU, "");
    if (StringUtils.isEmpty(parameterServicesGpu)) {
      setInptPropertiesWarn(sbMessage, SubmarineConstants.PARAMETER_SERVICES_GPU);
    }
    String parameterServicesCpu = properties.getProperty(
        SubmarineConstants.PARAMETER_SERVICES_CPU, "");
    if (StringUtils.isEmpty(parameterServicesCpu)) {
      setInptPropertiesWarn(sbMessage, SubmarineConstants.PARAMETER_SERVICES_CPU);
    }
    String parameterServicesMemory = properties.getProperty(
        SubmarineConstants.PARAMETER_SERVICES_MEMORY, "");
    if (StringUtils.isEmpty(parameterServicesMemory)) {
      setInptPropertiesWarn(sbMessage, SubmarineConstants.PARAMETER_SERVICES_MEMORY);
    }
    String workerServicesImage = properties.getProperty(
        SubmarineConstants.WORKER_SERVICES_DOCKER_IMAGE, "");
    if (StringUtils.isEmpty(workerServicesImage)) {
      setInptPropertiesWarn(sbMessage, SubmarineConstants.WORKER_SERVICES_DOCKER_IMAGE);
    }
    String workerServicesNum = properties.getProperty(
        SubmarineConstants.WORKER_SERVICES_NUM, "");
    if (StringUtils.isEmpty(workerServicesNum)) {
      setInptPropertiesWarn(sbMessage, SubmarineConstants.WORKER_SERVICES_NUM);
    }
    String workerServicesGpu = properties.getProperty(
        SubmarineConstants.WORKER_SERVICES_GPU, "");
    if (StringUtils.isEmpty(workerServicesGpu)) {
      setInptPropertiesWarn(sbMessage, SubmarineConstants.WORKER_SERVICES_GPU);
    }
    String workerServicesCpu = properties.getProperty(
        SubmarineConstants.WORKER_SERVICES_CPU, "");
    if (StringUtils.isEmpty(workerServicesCpu)) {
      setInptPropertiesWarn(sbMessage, SubmarineConstants.WORKER_SERVICES_CPU);
    }
    String workerServicesMemory = properties.getProperty(
        SubmarineConstants.WORKER_SERVICES_MEMORY, "");
    if (StringUtils.isEmpty(workerServicesMemory)) {
      setInptPropertiesWarn(sbMessage, SubmarineConstants.WORKER_SERVICES_MEMORY);
    }

    if (StringUtils.isEmpty(hadoopHome)) {
      setInptPropertiesWarn(sbMessage, SubmarineConstants.HADOOP_HOME);
    }

    if (StringUtils.isEmpty(submarineJar)) {
      setInptPropertiesWarn(sbMessage, SubmarineConstants.HADOOP_YARN_SUBMARINE_JAR);
    }

    String algorithmFileFullPath = submarineContext.getPropertie(noteId,
        SubmarineConstants.ALGORITHM_FILE_FULL_PATH);
    if (StringUtils.isEmpty(algorithmFileFullPath)) {
      sbMessage.append("Please first click the [RUN] button in the %submarine paragraph. " +
          "Upload the code to HDFS.\n");
    } else if (!submarineContext.getHDFSUtils().exists(new Path(algorithmFileFullPath))) {
      sbMessage.append("The " + algorithmFileFullPath + " file was not found in hdfs!\n");
      sbMessage.append("Please first click the [RUN] button in the %submarine paragraph. " +
          "Upload the code to HDFS.\n");
    }

    // Found null variable, throw exception
    if (!StringUtils.isEmpty(sbMessage.toString())) {
      throw new RuntimeException(sbMessage.toString());
    }

    // Save user-set variables and interpreter configuration parameters
    HashMap mapParams = new HashMap();
    mapParams.put(upperCaseKey(SubmarineConstants.HADOOP_HOME), hadoopHome);
    mapParams.put(upperCaseKey(SubmarineConstants.HADOOP_YARN_SUBMARINE_JAR), submarineJar);
    mapParams.put(upperCaseKey(SubmarineConstants.JOB_NAME), noteId);
    mapParams.put(upperCaseKey(SubmarineConstants.DOCKER_CONTAINER_NETWORK), containerNetwork);
    mapParams.put(upperCaseKey(SubmarineConstants.INPUT_PATH), inputPath);
    mapParams.put(upperCaseKey(SubmarineConstants.CHECKPOINT_PATH), checkPointPath);
    mapParams.put(upperCaseKey(SubmarineConstants.PS_LAUNCH_CMD), psLaunchCmd);
    mapParams.put(upperCaseKey(SubmarineConstants.WORKER_LAUNCH_CMD), workerLaunchCmd);
    mapParams.put(upperCaseKey(SubmarineConstants.PARAMETER_SERVICES_DOCKER_IMAGE),
        parameterServicesImage);
    mapParams.put(upperCaseKey(SubmarineConstants.PARAMETER_SERVICES_NUM), parameterServicesNum);
    mapParams.put(upperCaseKey(SubmarineConstants.PARAMETER_SERVICES_GPU), parameterServicesGpu);
    mapParams.put(upperCaseKey(SubmarineConstants.PARAMETER_SERVICES_CPU), parameterServicesCpu);
    mapParams.put(upperCaseKey(SubmarineConstants.PARAMETER_SERVICES_MEMORY),
        parameterServicesMemory);
    mapParams.put(upperCaseKey(SubmarineConstants.WORKER_SERVICES_DOCKER_IMAGE),
        workerServicesImage);
    mapParams.put(upperCaseKey(SubmarineConstants.WORKER_SERVICES_NUM), workerServicesNum);
    mapParams.put(upperCaseKey(SubmarineConstants.WORKER_SERVICES_GPU), workerServicesGpu);
    mapParams.put(upperCaseKey(SubmarineConstants.WORKER_SERVICES_CPU), workerServicesCpu);
    mapParams.put(upperCaseKey(SubmarineConstants.WORKER_SERVICES_MEMORY), workerServicesMemory);
    mapParams.put(
        upperCaseKey(SubmarineConstants.ALGORITHM_FILE_FULL_PATH), algorithmFileFullPath);

    return mapParams;
  }

  private String upperCaseKey(String key) {
    key = key.replace(".", "_").toUpperCase();
    return key;
  }

  private InterpreterResult jobShow(String jobName, InterpreterOutput output,
                                    OutputStream outStream)
      throws IOException {
    String yarnPath = hadoopHome + "/bin/yarn";
    File file = new File(yarnPath);
    if (!file.exists()) {
      output.write("ERROR：Yarn file does not exist！" + yarnPath);
      throw new RuntimeException(SubmarineConstants.HADOOP_HOME
          + " is not specified in interpreter-setting");
    }

    StringBuffer subamrineCmd = new StringBuffer();
    subamrineCmd.append(yarnPath).append(" ");
    subamrineCmd.append(submarineJar).append(" ");
    subamrineCmd.append("job show --name").append(" ");
    subamrineCmd.append(jobName);

    LOGGER.info("Execute : " + subamrineCmd.toString());
    output.write("Execute : " + subamrineCmd.toString());

    String cmd = "echo > " + subamrineCmd;

    CommandLine cmdLine = CommandLine.parse(shell);
    cmdLine.addArgument(cmd, false);

    DefaultExecutor executor = new DefaultExecutor();
    executor.setStreamHandler(new PumpStreamHandler(output, output));

    long timeout = Long.valueOf(getProperty(TIMEOUT_PROPERTY, defaultTimeoutProperty));

    executor.setWatchdog(new ExecuteWatchdog(timeout));
    if (Boolean.valueOf(getProperty(DIRECTORY_USER_HOME))) {
      executor.setWorkingDirectory(new File(System.getProperty("user.home")));
    }

    int exitVal = executor.execute(cmdLine);
    LOGGER.info("jobName {} return with exit value: {}", jobName, exitVal);
    return new InterpreterResult(InterpreterResult.Code.SUCCESS, outStream.toString());
  }

  private String getSubmarineHelp() {
    StringBuilder helpMsg = new StringBuilder();
    helpMsg.append("\n\nUsage: <object> [<action>] [<args>]\n");
    helpMsg.append("  Below are all objects / actions:\n");
    helpMsg.append("    job \n");
    helpMsg.append("       run : run a job, please see 'job run --help' for usage \n");
    helpMsg.append("       show : get status of job, please see 'job show --help' for usage \n");

    return helpMsg.toString();
  }
}
