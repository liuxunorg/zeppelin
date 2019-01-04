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
import org.apache.zeppelin.submarine.statemachine.SubmarineJob;
import org.apache.zeppelin.submarine.utils.SubmarineCommand;
import org.apache.zeppelin.submarine.utils.SubmarineConstants;
import org.apache.zeppelin.submarine.utils.SubmarineUtils;
import org.apache.zeppelin.submarine.utils.YarnClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.apache.zeppelin.submarine.utils.SubmarineUtils.unifyKey;

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


  private YarnClient yarnClient = null;

  // Submarine WEB UI
  private Map<String, String> mapMenuUI = new HashMap<>();

  SubmarineContext submarineContext = null;

  private float preActiveCount = 0;


  public SubmarineInterpreter(Properties properties) {
    super(properties);

    concurrentExecutedMax = Integer.parseInt(
        getProperty(SubmarineConstants.SUBMARINE_CONCURRENT_MAX, "1"));

    submarineContext = SubmarineContext.getInstance();

    yarnClient = new YarnClient();
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

  public InterpreterResult interpret1(String script, InterpreterContext context) {
    SubmarineJob submarineJob = submarineContext.addOrGetSubmarineJob(properties, context);
    submarineJob.onJobRun(true);

    return new InterpreterResult(InterpreterResult.Code.SUCCESS);
  }

  @Override
  public InterpreterResult interpret(String script, InterpreterContext context) {
    try {

      SubmarineJob submarineJob = submarineContext.addOrGetSubmarineJob(properties, context);

      setParagraphConfig(context);

      String command = "", activeCount = "";
      String inputPath = "", chkPntPath = "", psLaunchCmd = "", workerLaunchCmd = "";

      LOGGER.debug("Run shell command '" + script + "'");
      String noteId = context.getNoteId();
      String paragraphId = context.getParagraphId();
      String jobName = SubmarineUtils.getJobName(noteId);

      if (script.equalsIgnoreCase(SubmarineConstants.COMMAND_CLEAN)) {
        // Clean Registry Angular Object
        context.getAngularObjectRegistry().removeAll(context.getNoteId(), paragraphId);
      } else {
        command = SubmarineUtils.getAngularObjectValue(context, SubmarineConstants.COMMAND_TYPE);
      }
      boolean isActive = false;
      activeCount = SubmarineUtils.getAngularObjectValue(context,
          SubmarineConstants.COMMAND_ACTIVE);
      if (!StringUtils.isEmpty(activeCount)) {
        try {
          float curActiveCount = Float.parseFloat(activeCount);
          if (curActiveCount != preActiveCount) {
            this.preActiveCount = curActiveCount;
            isActive = true;
          }
        } catch (NumberFormatException e) {
          // clean
          SubmarineUtils.setAngularObjectValue(context, SubmarineConstants.COMMAND_TYPE, "");
        }
      }

      inputPath = SubmarineUtils.getAngularObjectValue(context, SubmarineConstants.INPUT_PATH);
      chkPntPath = SubmarineUtils.getAngularObjectValue(context,
          SubmarineConstants.CHECKPOINT_PATH);
      psLaunchCmd = SubmarineUtils.getAngularObjectValue(context, SubmarineConstants.PS_LAUNCH_CMD);
      workerLaunchCmd = SubmarineUtils.getAngularObjectValue(
          context, SubmarineConstants.WORKER_LAUNCH_CMD);
      properties.put(SubmarineConstants.INPUT_PATH, inputPath != null ? inputPath : "");
      properties.put(SubmarineConstants.CHECKPOINT_PATH, chkPntPath != null ? chkPntPath : "");
      properties.put(SubmarineConstants.PS_LAUNCH_CMD, psLaunchCmd != null ? psLaunchCmd : "");
      properties.put(SubmarineConstants.WORKER_LAUNCH_CMD,
          workerLaunchCmd != null ? workerLaunchCmd : "");

      SubmarineCommand submarineCmd = SubmarineCommand.fromCommand(command);
      switch (submarineCmd) {
        case USAGE:
          submarineJob.onShowUsage();
          break;
        case JOB_RUN:
          submarineJob.onJobRun(isActive);
          break;
        case JOB_SHOW:
          submarineJob.onJobShow(isActive);
          break;
        case OLD_UI:
          createOldGUI(context);
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
    String jobName = SubmarineUtils.getJobName(context.getNoteId());
    submarineCommand(SubmarineCommand.JOB_CANCEL, jobName, context);
  }

  @Override
  public FormType getFormType() {
    return FormType.SIMPLE;
  }

  @Override
  public int getProgress(InterpreterContext context) {
    String jobName = SubmarineUtils.getJobName(context.getNoteId());
    Map<String, Object> mapStatus = yarnClient.getAppStatus(jobName);

    if (mapStatus.containsKey(YarnClient.APPLICATION_ID)
        && mapStatus.containsKey(YarnClient.APPLICATION_NAME)
        && mapStatus.containsKey(YarnClient.APPLICATION_STATUS)) {
      String appId = mapStatus.get(YarnClient.APPLICATION_ID).toString();
      String appName = mapStatus.get(YarnClient.APPLICATION_NAME).toString();
      String appStatus = mapStatus.get(YarnClient.APPLICATION_STATUS).toString();

      if (!mapMenuUI.containsKey(YarnClient.APPLICATION_ID)) {
        // create YARN UI link
        StringBuffer sbUrl = new StringBuffer();
        String yarnBaseUrl = properties.getProperty(SubmarineConstants.YARN_WEB_HTTP_ADDRESS, "");
        sbUrl.append(yarnBaseUrl);
        sbUrl.append("/ui2/#/yarn-app/");
        sbUrl.append(appId);
        sbUrl.append("/components?service=");
        sbUrl.append(appName);

        Map<String, String> infos = new java.util.HashMap<>();
        infos.put("jobUrl", sbUrl.toString());
        infos.put("jobLabel", "Yarn log");
        infos.put("label", "Submarine WEB");
        infos.put("tooltip", "View in Submarine web UI");
        infos.put("noteId", context.getNoteId());
        infos.put("paraId", context.getParagraphId());
        context.getIntpEventClient().onParaInfosReceived(infos);

        Map<String, String> infos2 = new java.util.HashMap<>();
        infos2.put("jobUrl", "http://192.168.0.1/tensorboard");
        infos2.put("jobLabel", "Tensorboard");
        infos2.put("label", "Submarine WEB");
        infos2.put("tooltip", "View in Submarine web UI");
        infos2.put("noteId", context.getNoteId());
        infos2.put("paraId", context.getParagraphId());
        context.getIntpEventClient().onParaInfosReceived(infos2);

        mapMenuUI.put(YarnClient.APPLICATION_ID, sbUrl.toString());
      }

      if (StringUtils.equals(appStatus, YarnClient.APPLICATION_STATUS_FINISHED)
          || StringUtils.equals(appStatus, YarnClient.APPLICATION_STATUS_FAILED)) {
        return 0;
      } else if (StringUtils.equals(appStatus, YarnClient.APPLICATION_STATUS_ACCEPT)) {
        return 1;
      } else if (StringUtils.equals(appStatus, YarnClient.APPLICATION_STATUS_RUNNING)) {
        return 10;
      }
    }

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
  public List<InterpreterCompletion> completion(
      String buf, int cursor, InterpreterContext interpreterContext) {
    return null;
  }

  public void setPythonWorkDir(String noteId, File pythonWorkDir) {
    SubmarineJob submarineJob = submarineContext.getSubmarineJob(noteId);
    if (null != submarineJob) {
      submarineJob.setPythonWorkDir(pythonWorkDir);
    }
  }

  private InterpreterResult jobList(String jobName, String noteId,
                                    InterpreterContext context)
      throws IOException {
    return new InterpreterResult(InterpreterResult.Code.SUCCESS, context.out.toString());
  }

  private InterpreterResult jobShow(SubmarineCommand command, String jobName, String noteId,
                                    InterpreterContext context) {
    Map<String, Object> mapStatus = yarnClient.getAppStatus(jobName);

    return new InterpreterResult(InterpreterResult.Code.SUCCESS);
  }

  private InterpreterResult submarineCommand(
      SubmarineCommand command, String jobName, InterpreterContext context) {
    SubmarineJob submarineJob = submarineContext.getSubmarineJob(context.getNoteId());

    HashMap jinjaParams = null;
    try {
      jinjaParams = SubmarineUtils.propertiesToJinjaParams(submarineJob.getProperties(),
          submarineJob.getSubmarineUI(), submarineJob.getHdfsUtils(),
          jobName, false);
      jinjaParams.put(unifyKey(SubmarineConstants.JOB_NAME), jobName);
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
      submarineJob.getSubmarineUI().outputLog(null, sbLogOutput.toString());
    } catch (IOException e) {
      e.printStackTrace();
    }

    return new InterpreterResult(InterpreterResult.Code.SUCCESS);
  }
}
