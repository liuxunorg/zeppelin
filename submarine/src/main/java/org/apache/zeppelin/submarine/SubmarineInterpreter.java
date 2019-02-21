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
import org.apache.commons.exec.ExecuteWatchdog;
import org.apache.commons.exec.LogOutputStream;
import org.apache.commons.exec.PumpStreamHandler;
import org.apache.commons.lang.StringUtils;
import org.apache.zeppelin.display.ui.OptionInput.ParamOption;
import org.apache.zeppelin.interpreter.Interpreter;
import org.apache.zeppelin.interpreter.InterpreterContext;
import org.apache.zeppelin.interpreter.InterpreterResult;
import org.apache.zeppelin.interpreter.thrift.InterpreterCompletion;
import org.apache.zeppelin.scheduler.Scheduler;
import org.apache.zeppelin.scheduler.SchedulerFactory;
import org.apache.zeppelin.submarine.componts.SubmarineCommand;
import org.apache.zeppelin.submarine.componts.SubmarineConstants;
import org.apache.zeppelin.submarine.componts.SubmarineJob;
import org.apache.zeppelin.submarine.componts.SubmarineUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;

import static org.apache.zeppelin.submarine.componts.SubmarineCommand.CLEAN_RUNTIME_CACHE;
import static org.apache.zeppelin.submarine.componts.SubmarineUtils.unifyKey;

/**
 * SubmarineInterpreter of Hadoop Submarine implementation.
 * Support for Hadoop Submarine cli. All the commands documented here
 * https://github.com/apache/hadoop/blob/trunk/hadoop-yarn-project/hadoop-yarn/
 * hadoop-yarn-applications/hadoop-yarn-submarine/src/site/markdown/QuickStart.md is supported.
 */
public class SubmarineInterpreter extends Interpreter {
  private Logger LOGGER = LoggerFactory.getLogger(SubmarineInterpreter.class);

  // Number of submarines executed in parallel for each interpreter instance
  protected int concurrentExecutedMax = 1;

  private static final String DIRECTORY_USER_HOME = "shell.working.directory.user.home";
  private final boolean isWindows = System.getProperty("os.name").startsWith("Windows");
  private final String shell = isWindows ? "cmd /c" : "bash -c";

  private static final String TIMEOUT_PROPERTY = "submarine.command.timeout.millisecond";
  private String defaultTimeoutProperty = "60000";

  private boolean needUpdateConfig = true;
  private String currentReplName = "";

  SubmarineContext submarineContext = null;

  public SubmarineInterpreter(Properties properties) {
    super(properties);

    concurrentExecutedMax = Integer.parseInt(
        getProperty(SubmarineConstants.SUBMARINE_CONCURRENT_MAX, "1"));

    submarineContext = SubmarineContext.getInstance();
  }

  @Override
  public void open() {
    LOGGER.info("Command timeout property: {}", getProperty(TIMEOUT_PROPERTY));
  }

  @Override
  public void close() {
  }

  private String createOldGUI(InterpreterContext context) {
    // submarine command - Format
    ParamOption[] commandOptions = new ParamOption[4];
    commandOptions[0] = new ParamOption(SubmarineConstants.COMMAND_JOB_RUN,
        SubmarineConstants.COMMAND_JOB_RUN);
    commandOptions[1] = new ParamOption(SubmarineConstants.COMMAND_JOB_SHOW,
        SubmarineConstants.COMMAND_JOB_SHOW);
    commandOptions[2] = new ParamOption(SubmarineConstants.COMMAND_USAGE,
        SubmarineConstants.COMMAND_USAGE);
    String command = (String) context.getGui().
        select("Submarine Command", "", commandOptions);

    String distributed = this.properties.getProperty(
        SubmarineConstants.MACHINELEARING_DISTRIBUTED_ENABLE, "false");

    if (command.equals(SubmarineConstants.COMMAND_JOB_RUN)) {
      String inputPath = (String) context.getGui().textbox("Input Path(input_path)");
      String checkpoinkPath = (String) context.getGui().textbox("Checkpoint Path(checkpoint_path)");
      if (distributed.equals("true")) {
        String psLaunchCmd = (String) context.getGui().textbox("PS Launch Command");
      }
      String workerLaunchCmd = (String) context.getGui().textbox("Worker Launch Command");
    }

    /* Active
    ParamOption[] auditOptions = new ParamOption[1];
    auditOptions[0] = new ParamOption("Active", "Active command");
    List<Object> flags = intpContext.getGui().checkbox("Active", Arrays.asList(""), auditOptions);
    boolean activeChecked = flags.contains("Active");
    intpContext.getResourcePool().put(intpContext.getNoteId(),
        intpContext.getParagraphId(), "Active", activeChecked);*/

    return command;
  }

  private void setParagraphConfig(InterpreterContext context) {
    String replName = context.getReplName();
    if (StringUtils.equals(currentReplName, replName)) {
      currentReplName = context.getReplName();
      needUpdateConfig = true;
    }
    if (needUpdateConfig) {
      needUpdateConfig = false;
      if (currentReplName.equals("submarine") || currentReplName.isEmpty()) {
        context.getConfig().put("editorHide", true);
        context.getConfig().put("title", false);
      } else {
        context.getConfig().put("editorHide", false);
        context.getConfig().put("title", true);
      }
    }
  }

  @Override
  public InterpreterResult interpret(String script, InterpreterContext context) {
    try {
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

      SubmarineJob submarineJob = submarineContext.addOrGetSubmarineJob(properties, context);

      setParagraphConfig(context);

      LOGGER.debug("Run shell command '" + script + "'");
      String command = "", operation = "", cleanCheckpoint = "";
      String inputPath = "", chkPntPath = "", psLaunchCmd = "", workerLaunchCmd = "";
      String noteId = context.getNoteId();
      String noteName = context.getNoteName();

      if (script.equalsIgnoreCase(SubmarineConstants.COMMAND_CLEAN)) {
        // Clean Registry Angular Object
        command = CLEAN_RUNTIME_CACHE.getCommand();
      } else {
        operation = SubmarineUtils.getAgulObjValue(context, SubmarineConstants.OPERATION_TYPE);
        if (!StringUtils.isEmpty(operation)) {
          SubmarineUtils.removeAgulObjValue(context, SubmarineConstants.OPERATION_TYPE);
          command = operation;
        } else {
          command = SubmarineUtils.getAgulObjValue(context, SubmarineConstants.COMMAND_TYPE);
        }
      }

      String distributed = this.properties.getProperty(
          SubmarineConstants.MACHINELEARING_DISTRIBUTED_ENABLE, "false");
      SubmarineUtils.setAgulObjValue(context,
          unifyKey(SubmarineConstants.MACHINELEARING_DISTRIBUTED_ENABLE), distributed);

      inputPath = SubmarineUtils.getAgulObjValue(context, SubmarineConstants.INPUT_PATH);
      cleanCheckpoint = SubmarineUtils.getAgulObjValue(context,
          SubmarineConstants.CLEAN_CHECKPOINT);
      chkPntPath = submarineJob.getJobDefaultCheckpointPath();
      SubmarineUtils.setAgulObjValue(context, SubmarineConstants.CHECKPOINT_PATH, chkPntPath);
      psLaunchCmd = SubmarineUtils.getAgulObjValue(context, SubmarineConstants.PS_LAUNCH_CMD);
      workerLaunchCmd = SubmarineUtils.getAgulObjValue(context,
          SubmarineConstants.WORKER_LAUNCH_CMD);
      properties.put(SubmarineConstants.INPUT_PATH, inputPath != null ? inputPath : "");
      properties.put(SubmarineConstants.CHECKPOINT_PATH, chkPntPath != null ? chkPntPath : "");
      properties.put(SubmarineConstants.PS_LAUNCH_CMD, psLaunchCmd != null ? psLaunchCmd : "");
      properties.put(SubmarineConstants.WORKER_LAUNCH_CMD,
          workerLaunchCmd != null ? workerLaunchCmd : "");

      SubmarineCommand submarineCmd = SubmarineCommand.fromCommand(command);
      switch (submarineCmd) {
        case USAGE:
          submarineJob.showUsage();
          break;
        case JOB_RUN:
          if (StringUtils.equals(cleanCheckpoint, "true")) {
            submarineJob.cleanJobDefaultCheckpointPath();
          }
          submarineJob.runJob();
          break;
        case JOB_STOP:
          String jobName = SubmarineUtils.getJobName(userName, noteId);
          submarineJob.deleteJob(jobName);
          break;
        case TENSORBOARD_RUN:
          submarineJob.runTensorBoard();
          break;
        case TENSORBOARD_STOP:
          String user = context.getAuthenticationInfo().getUser();
          String tensorboardName = SubmarineUtils.getTensorboardName(user);
          submarineJob.deleteJob(tensorboardName);
          break;
        case OLD_UI:
          createOldGUI(context);
          break;
        case CLEAN_RUNTIME_CACHE:
          submarineJob.cleanRuntimeCache();
          break;
        default:
          submarineJob.onDashboard();
          break;
      }
    } catch (Exception e) {
      LOGGER.error(e.getMessage());
      return new InterpreterResult(InterpreterResult.Code.ERROR, e.getMessage());
    }

    return new InterpreterResult(InterpreterResult.Code.SUCCESS);
  }

  @Override
  public void cancel(InterpreterContext context) {
    SubmarineJob submarineJob = submarineContext.addOrGetSubmarineJob(properties, context);
    String userName = context.getAuthenticationInfo().getUser();
    String noteId = context.getNoteId();
    String jobName = SubmarineUtils.getJobName(userName, noteId);
    submarineJob.deleteJob(jobName);
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
    String schedulerName = SubmarineInterpreter.class.getName() + this.hashCode();
    if (concurrentExecutedMax > 1) {
      return SchedulerFactory.singleton().createOrGetParallelScheduler(schedulerName,
          concurrentExecutedMax);
    } else {
      return SchedulerFactory.singleton().createOrGetFIFOScheduler(schedulerName);
    }
  }

  @Override
  public List<InterpreterCompletion> completion(String buf, int cursor,
                                                InterpreterContext interpreterContext) {
    return null;
  }

  public void setPythonWorkDir(String noteId, File pythonWorkDir) {
    SubmarineJob submarineJob = submarineContext.getSubmarineJob(noteId);
    if (null != submarineJob) {
      submarineJob.setPythonWorkDir(pythonWorkDir);
    }
  }

  private void submarineCommand(SubmarineCommand command, InterpreterContext context) {
    SubmarineJob submarineJob = submarineContext.getSubmarineJob(context.getNoteId());

    HashMap jinjaParams = null;
    try {
      jinjaParams = SubmarineUtils.propertiesToJinjaParams(submarineJob.getProperties(),
          submarineJob, false);
      String noteId = context.getNoteId();
      String jobName = SubmarineUtils.getJobName(userName, noteId);
      String tensorboardName = SubmarineUtils.getTensorboardName(userName);

      switch (command) {
        case JOB_STOP:
          jinjaParams.put(unifyKey(SubmarineConstants.JOB_NAME), jobName);
          break;
        case TENSORBOARD_STOP:
          jinjaParams.put(unifyKey(SubmarineConstants.JOB_NAME), tensorboardName);
          break;
        default:
          LOGGER.error("Unsupported operation！");
          break;
      }
      jinjaParams.put(unifyKey(SubmarineConstants.COMMAND_TYPE), command.getCommand());

      URL urlTemplate = Resources.getResource(SubmarineJob.SUBMARINE_COMMAND_JINJA);
      String template = Resources.toString(urlTemplate, Charsets.UTF_8);
      Jinjava jinjava = new Jinjava();
      String submarineCmd = jinjava.render(template, jinjaParams);

      LOGGER.info("Execute : " + submarineCmd);
      submarineJob.getSubmarineUI().outputLog("Submarine submit command", submarineCmd);
      CommandLine cmdLine = CommandLine.parse(shell);
      cmdLine.addArgument(submarineCmd, false);

      DefaultExecutor executor = new DefaultExecutor();

      StringBuffer sbLogOutput = new StringBuffer();
      executor.setStreamHandler(new PumpStreamHandler(new LogOutputStream() {
        @Override
        protected void processLine(String line, int level) {
          sbLogOutput.append(line);
        }
      }));

      // executor.setStreamHandler(new PumpStreamHandler(interpreterOutput, interpreterOutput));
      long timeout = Long.valueOf(getProperty(TIMEOUT_PROPERTY, defaultTimeoutProperty));

      executor.setWatchdog(new ExecuteWatchdog(timeout));
      if (Boolean.valueOf(getProperty(DIRECTORY_USER_HOME))) {
        executor.setWorkingDirectory(new File(System.getProperty("user.home")));
      }

      int exitVal = executor.execute(cmdLine);
      LOGGER.info("jobName {} return with exit value: {}", jobName, exitVal);
      submarineJob.getSubmarineUI().outputLog(command.getCommand(), sbLogOutput.toString());
    } catch (IOException e) {
      LOGGER.error(e.getMessage(), e);
    }
  }
}
