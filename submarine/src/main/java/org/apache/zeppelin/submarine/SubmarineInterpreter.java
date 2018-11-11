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
import org.apache.zeppelin.conf.ZeppelinConfiguration;
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

import java.io.File;
import java.io.OutputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
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

  private ZeppelinConfiguration zconf;
  private File pythonWorkDir = null;

  private String submarineJobRunTFJinja = "submarine-job-run-tf.jinja";

  // Number of submarines executed in parallel for each interpreter instance
  protected int concurrentExecutedMax = 1;

  private static final String DIRECTORY_USER_HOME = "shell.working.directory.user.home";
  private final boolean isWindows = System.getProperty("os.name").startsWith("Windows");
  private final String shell = isWindows ? "cmd /c" : "bash -c";

  private static final String TIMEOUT_PROPERTY = "submarine.command.timeout.millisecond";
  private String defaultTimeoutProperty = "60000";

  private String submarineHadoopHome;
  private String submarineJar;

  private SubmarineContext submarineContext = null;

  public SubmarineInterpreter(Properties property) {
    super(property);

    zconf = ZeppelinConfiguration.create();

    submarineContext = SubmarineContext.getInstance(properties);

    concurrentExecutedMax = Integer.parseInt(
        getProperty(SubmarineConstants.SUBMARINE_CONCURRENT_MAX, "1"));

    submarineHadoopHome = properties.getProperty(SubmarineConstants.SUBMARINE_HADOOP_HOME, "");
    if (StringUtils.isEmpty(submarineHadoopHome)) {
      LOGGER.error("Please set the submarine interpreter properties : " +
          SubmarineConstants.SUBMARINE_HADOOP_HOME);
    }
    File file = new File(submarineHadoopHome);
    if (!file.exists()) {
      LOGGER.error(submarineHadoopHome + " is not a valid file path!");
    }

    submarineJar = properties.getProperty(SubmarineConstants.HADOOP_YARN_SUBMARINE_JAR, "");
    if (StringUtils.isEmpty(submarineJar)) {
      LOGGER.error("Please set the submarine interpreter properties : " +
          SubmarineConstants.HADOOP_YARN_SUBMARINE_JAR);
    }
    file = new File(submarineJar);
    if (!file.exists()) {
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
  public InterpreterResult interpret(String script, InterpreterContext intpContext) {
    LOGGER.debug("Run shell command '" + script + "'");
    OutputStream outStream = new ByteArrayOutputStream();
    String jobName = getJobName(intpContext);
    String noteId = intpContext.getNoteId();

    try {
      Properties properties = submarineContext.getProperties(noteId);
      if (null == properties) {
        properties = this.properties;
        submarineContext.setProperties(noteId, properties);
        properties.put(SubmarineConstants.JOB_NAME, jobName);
      }

      String outputMsg = submarineContext.saveParagraphToFiles(noteId, intpContext.getNoteName(),
          pythonWorkDir == null ? "" : pythonWorkDir.getAbsolutePath());
      intpContext.out.write(outputMsg);

      CommandParser commandParser = new CommandParser();
      commandParser.populate(script);

      // Get the set variables from the user's execution script
      String inputPath = commandParser.getConfig(SubmarineConstants.TF_INPUT_PATH, "");
      String chkPntPath = commandParser.getConfig(SubmarineConstants.TF_CHECKPOINT_PATH, "");
      String psLaunchCmd = commandParser.getConfig(SubmarineConstants.TF_PS_LAUNCH_CMD, "");
      String workerLaunchCmd = commandParser.getConfig(SubmarineConstants.TF_WORKER_LAUNCH_CMD, "");
      properties.put(SubmarineConstants.TF_INPUT_PATH, inputPath != null ? inputPath : "");
      properties.put(SubmarineConstants.TF_CHECKPOINT_PATH, chkPntPath != null ? chkPntPath : "");
      properties.put(SubmarineConstants.TF_PS_LAUNCH_CMD, psLaunchCmd != null ? psLaunchCmd : "");
      properties.put(SubmarineConstants.TF_WORKER_LAUNCH_CMD,
          workerLaunchCmd != null ? workerLaunchCmd : "");

      String command = commandParser.getCommand();
      CommandLine cmdLine = CommandLine.parse(shell);
      cmdLine.addArgument(script, false);

      if (command.equalsIgnoreCase(SubmarineConstants.HELP_COMMAND)) {
        String message = getSubmarineHelp();
        return new InterpreterResult(InterpreterResult.Code.SUCCESS, message);
      } else if (command.equalsIgnoreCase(SubmarineConstants.COMMAND_JOB_SHOW)) {
        return jobShow(jobName, noteId, intpContext.out, outStream);
      } else if (command.equals(SubmarineConstants.COMMAND_JOB_RUN)) {
        return jobRun(jobName, noteId, intpContext.out, outStream);
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
        LOGGER.info("The paragraph " + intpContext.getParagraphId()
            + " stopped executing: " + message);
      }
      message += "ExitValue: " + exitValue;

      return new InterpreterResult(code, message);
    } catch (Exception e) {
      LOGGER.error("Can not run " + script, e);
      String message = outStream.toString() + "\n" + e.getMessage();
      return new InterpreterResult(InterpreterResult.Code.ERROR, message);
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

  public void setPythonWorkDir(File pythonWorkDir) {
    this.pythonWorkDir = pythonWorkDir;
  }

  public void createSecureConfiguration() throws InterpreterException {
    Properties properties = getProperties();
    CommandLine cmdLine = CommandLine.parse(shell);
    cmdLine.addArgument("-c", false);
    String kinitCommand = String.format("kinit -k -t %s %s",
        properties.getProperty(SubmarineConstants.SUBMARINE_HADOOP_KEYTAB),
        properties.getProperty(SubmarineConstants.SUBMARINE_HADOOP_PRINCIPAL));
    cmdLine.addArgument(kinitCommand, false);
    DefaultExecutor executor = new DefaultExecutor();
    try {
      executor.execute(cmdLine);
    } catch (Exception e) {
      LOGGER.error("Unable to run kinit for submarine user " + kinitCommand, e);
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


  // yarn application match the pattern [a-z][a-z0-9-]*
  private String getJobName(InterpreterContext contextIntp) {
    return "submarine-" + contextIntp.getNoteId().toLowerCase();
  }

  private InterpreterResult jobRun(String jobName, String noteId,
                                   InterpreterOutput output, OutputStream outStream)
      throws IOException {
    HashMap jinjaParams = propertiesToJinjaParams(jobName, noteId, output);

    URL urlTemplate = Resources.getResource(submarineJobRunTFJinja);
    String template = Resources.toString(urlTemplate, Charsets.UTF_8);
    Jinjava jinjava = new Jinjava();
    String submarineCmd = jinjava.render(template, jinjaParams);

    LOGGER.info("Execute : " + submarineCmd);
    output.write("Submarine submit job : " + jobName);
    CommandLine cmdLine = CommandLine.parse(shell);
    cmdLine.addArgument(submarineCmd, false);

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

  private StringBuffer setUserPropertiesWarn(StringBuffer sbMessage, String key, String info) {
    sbMessage.append("ERROR: Please set the parameter ");
    sbMessage.append(key);
    sbMessage.append(" first, \nfor example: ");
    sbMessage.append(key + info);

    return sbMessage;
  }

  private void setIntpPropertiesWarn(StringBuffer sbMessage, String key) {
    sbMessage.append("ERROR: Please set the submarine interpreter properties : ");
    sbMessage.append(key).append("\n");
  }

  // Convert properties to Map and check that the variable cannot be empty
  private HashMap propertiesToJinjaParams(String jobName, String noteId, InterpreterOutput output)
      throws IOException {
    StringBuffer sbMessage = new StringBuffer();

    // Check user-set job variables
    String inputPath = submarineContext.getPropertie(noteId,
        SubmarineConstants.TF_INPUT_PATH);
    if (StringUtils.isEmpty(inputPath)) {
      setUserPropertiesWarn(sbMessage, SubmarineConstants.TF_INPUT_PATH, "=path...\n");
    }
    String checkPointPath = submarineContext.getPropertie(noteId,
        SubmarineConstants.TF_CHECKPOINT_PATH);
    if (StringUtils.isEmpty(checkPointPath)) {
      setUserPropertiesWarn(sbMessage, SubmarineConstants.TF_CHECKPOINT_PATH, "=path...\n");
    }
    String psLaunchCmd = submarineContext.getPropertie(noteId,
        SubmarineConstants.TF_PS_LAUNCH_CMD);
    if (StringUtils.isEmpty(psLaunchCmd)) {
      setUserPropertiesWarn(sbMessage, SubmarineConstants.TF_PS_LAUNCH_CMD,
          "=python /test/cifar10_estimator/cifar10_main.py " +
          "--data-dir=hdfs://mldev/tmp/cifar-10-data " +
          "--job-dir=hdfs://mldev/tmp/cifar-10-jobdir --num-gpus=0\n");
    }
    String workerLaunchCmd = submarineContext.getPropertie(noteId,
        SubmarineConstants.TF_WORKER_LAUNCH_CMD);
    if (StringUtils.isEmpty(workerLaunchCmd)) {
      setUserPropertiesWarn(sbMessage, SubmarineConstants.TF_WORKER_LAUNCH_CMD,
          "=python /test/cifar10_estimator/cifar10_main.py " +
          "--data-dir=hdfs://mldev/tmp/cifar-10-data " +
          "--job-dir=hdfs://mldev/tmp/cifar-10-jobdir " +
          "--train-steps=500 --eval-batch-size=16 --train-batch-size=16 --sync --num-gpus=1\n");
    }

    // Check interpretere set Properties
    if (StringUtils.isEmpty(submarineHadoopHome)) {
      setIntpPropertiesWarn(sbMessage, SubmarineConstants.SUBMARINE_HADOOP_HOME);
    }
    File file = new File(submarineHadoopHome);
    if (!file.exists()) {
      sbMessage.append(SubmarineConstants.HADOOP_YARN_SUBMARINE_JAR + ": "
          + submarineHadoopHome + " is not a valid file path!\n");
    }
    if (StringUtils.isEmpty(submarineJar)) {
      setIntpPropertiesWarn(sbMessage, SubmarineConstants.HADOOP_YARN_SUBMARINE_JAR);
    }
    file = new File(submarineJar);
    if (!file.exists()) {
      sbMessage.append(SubmarineConstants.HADOOP_YARN_SUBMARINE_JAR + ":"
          + submarineJar + " is not a valid file path!\n");
    }
    String containerNetwork = properties.getProperty(
        SubmarineConstants.DOCKER_CONTAINER_NETWORK, "");
    if (StringUtils.isEmpty(containerNetwork)) {
      setIntpPropertiesWarn(sbMessage, SubmarineConstants.DOCKER_CONTAINER_NETWORK);
    }
    String parameterServicesImage = properties.getProperty(
        SubmarineConstants.TF_PARAMETER_SERVICES_DOCKER_IMAGE, "");
    if (StringUtils.isEmpty(parameterServicesImage)) {
      setIntpPropertiesWarn(sbMessage, SubmarineConstants.TF_PARAMETER_SERVICES_DOCKER_IMAGE);
    }
    String parameterServicesNum = properties.getProperty(
        SubmarineConstants.TF_PARAMETER_SERVICES_NUM, "");
    if (StringUtils.isEmpty(parameterServicesNum)) {
      setIntpPropertiesWarn(sbMessage, SubmarineConstants.TF_PARAMETER_SERVICES_NUM);
    }
    String parameterServicesGpu = properties.getProperty(
        SubmarineConstants.TF_PARAMETER_SERVICES_GPU, "");
    if (StringUtils.isEmpty(parameterServicesGpu)) {
      setIntpPropertiesWarn(sbMessage, SubmarineConstants.TF_PARAMETER_SERVICES_GPU);
    }
    String parameterServicesCpu = properties.getProperty(
        SubmarineConstants.TF_PARAMETER_SERVICES_CPU, "");
    if (StringUtils.isEmpty(parameterServicesCpu)) {
      setIntpPropertiesWarn(sbMessage, SubmarineConstants.TF_PARAMETER_SERVICES_CPU);
    }
    String parameterServicesMemory = properties.getProperty(
        SubmarineConstants.TF_PARAMETER_SERVICES_MEMORY, "");
    if (StringUtils.isEmpty(parameterServicesMemory)) {
      setIntpPropertiesWarn(sbMessage, SubmarineConstants.TF_PARAMETER_SERVICES_MEMORY);
    }
    String workerServicesImage = properties.getProperty(
        SubmarineConstants.TF_WORKER_SERVICES_DOCKER_IMAGE, "");
    if (StringUtils.isEmpty(workerServicesImage)) {
      setIntpPropertiesWarn(sbMessage, SubmarineConstants.TF_WORKER_SERVICES_DOCKER_IMAGE);
    }
    String workerServicesNum = properties.getProperty(
        SubmarineConstants.TF_WORKER_SERVICES_NUM, "");
    if (StringUtils.isEmpty(workerServicesNum)) {
      setIntpPropertiesWarn(sbMessage, SubmarineConstants.TF_WORKER_SERVICES_NUM);
    }
    String workerServicesGpu = properties.getProperty(
        SubmarineConstants.TF_WORKER_SERVICES_GPU, "");
    if (StringUtils.isEmpty(workerServicesGpu)) {
      setIntpPropertiesWarn(sbMessage, SubmarineConstants.TF_WORKER_SERVICES_GPU);
    }
    String workerServicesCpu = properties.getProperty(
        SubmarineConstants.TF_WORKER_SERVICES_CPU, "");
    if (StringUtils.isEmpty(workerServicesCpu)) {
      setIntpPropertiesWarn(sbMessage, SubmarineConstants.TF_WORKER_SERVICES_CPU);
    }
    String workerServicesMemory = properties.getProperty(
        SubmarineConstants.TF_WORKER_SERVICES_MEMORY, "");
    if (StringUtils.isEmpty(workerServicesMemory)) {
      setIntpPropertiesWarn(sbMessage, SubmarineConstants.TF_WORKER_SERVICES_MEMORY);
    }
    String algorithmUploadPath = getProperty(
        SubmarineConstants.SUBMARINE_ALGORITHM_HDFS_PATH, "");
    if (StringUtils.isEmpty(algorithmUploadPath)) {
      setIntpPropertiesWarn(sbMessage, SubmarineConstants.SUBMARINE_ALGORITHM_HDFS_PATH);
    }
    String notePath = algorithmUploadPath + File.separator + noteId + File.separator + "*";
    List<Path> files = submarineContext.getHDFSUtils().list(new Path(notePath));
    if (files.size() == 0) {
      sbMessage.append("ERROR: The " + notePath + " file directory was is empty in HDFS!\n");
    } else {
      output.write("INFO: You commit total of " + files.size() + " algorithm files.\n");
      for (int i = 0; i < files.size(); i++) {
        output.write("INFO: [" + files.get(i).getName() + "] -> "
            + files.get(i).toUri().getPath() + "\n");
      }
    }

    // Found null variable, throw exception
    if (!StringUtils.isEmpty(sbMessage.toString())) {
      throw new RuntimeException(sbMessage.toString());
    }

    // Save user-set variables and interpreter configuration parameters
    HashMap mapParams = new HashMap();
    mapParams.put(unifyKey(SubmarineConstants.SUBMARINE_HADOOP_HOME), submarineHadoopHome);
    mapParams.put(unifyKey(SubmarineConstants.HADOOP_YARN_SUBMARINE_JAR), submarineJar);
    mapParams.put(unifyKey(SubmarineConstants.JOB_NAME), jobName);
    mapParams.put(unifyKey(SubmarineConstants.DOCKER_CONTAINER_NETWORK), containerNetwork);
    mapParams.put(unifyKey(SubmarineConstants.TF_INPUT_PATH), inputPath);
    mapParams.put(unifyKey(SubmarineConstants.TF_CHECKPOINT_PATH), checkPointPath);
    mapParams.put(unifyKey(SubmarineConstants.TF_PS_LAUNCH_CMD), psLaunchCmd);
    mapParams.put(unifyKey(SubmarineConstants.TF_WORKER_LAUNCH_CMD), workerLaunchCmd);
    mapParams.put(unifyKey(SubmarineConstants.TF_PARAMETER_SERVICES_DOCKER_IMAGE),
        parameterServicesImage);
    mapParams.put(unifyKey(SubmarineConstants.TF_PARAMETER_SERVICES_NUM), parameterServicesNum);
    mapParams.put(unifyKey(SubmarineConstants.TF_PARAMETER_SERVICES_GPU), parameterServicesGpu);
    mapParams.put(unifyKey(SubmarineConstants.TF_PARAMETER_SERVICES_CPU), parameterServicesCpu);
    mapParams.put(unifyKey(SubmarineConstants.TF_PARAMETER_SERVICES_MEMORY),
        parameterServicesMemory);
    mapParams.put(unifyKey(SubmarineConstants.TF_WORKER_SERVICES_DOCKER_IMAGE),
        workerServicesImage);
    mapParams.put(unifyKey(SubmarineConstants.TF_WORKER_SERVICES_NUM), workerServicesNum);
    mapParams.put(unifyKey(SubmarineConstants.TF_WORKER_SERVICES_GPU), workerServicesGpu);
    mapParams.put(unifyKey(SubmarineConstants.TF_WORKER_SERVICES_CPU), workerServicesCpu);
    mapParams.put(unifyKey(SubmarineConstants.TF_WORKER_SERVICES_MEMORY), workerServicesMemory);

    return mapParams;
  }

  private String unifyKey(String key) {
    key = key.replace(".", "_").toUpperCase();
    return key;
  }

  private InterpreterResult jobShow(String jobName, String noteId,
                                    InterpreterOutput output, OutputStream outStream)
      throws IOException {
    String yarnPath = submarineHadoopHome + "/bin/yarn";
    File file = new File(yarnPath);
    if (!file.exists()) {
      output.write("ERROR：Yarn file does not exist！" + yarnPath);
      throw new RuntimeException(SubmarineConstants.SUBMARINE_HADOOP_HOME
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
