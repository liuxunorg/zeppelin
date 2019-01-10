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

package org.apache.zeppelin.submarine.statemachine;

import com.google.common.io.Resources;
import com.hubspot.jinjava.Jinjava;
import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.ExecuteWatchdog;
import org.apache.commons.exec.LogOutputStream;
import org.apache.commons.exec.PumpStreamHandler;
import org.apache.commons.io.Charsets;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.zeppelin.interpreter.InterpreterContext;
import org.apache.zeppelin.submarine.utils.HDFSUtils;
import org.apache.zeppelin.submarine.utils.SubmarineCommand;
import org.apache.zeppelin.submarine.utils.SubmarineConstants;
import org.apache.zeppelin.submarine.utils.SubmarineUI;
import org.apache.zeppelin.submarine.utils.SubmarineUtils;
import org.apache.zeppelin.submarine.utils.YarnClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.squirrelframework.foundation.fsm.StateMachineBuilder;
import org.squirrelframework.foundation.fsm.StateMachineBuilderFactory;
import org.squirrelframework.foundation.fsm.StateMachineConfiguration;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.zeppelin.submarine.statemachine.SubmarineJobStatus.EXECUTE_SUBMARINE_ERROR;
import static org.apache.zeppelin.submarine.statemachine.SubmarineJobStatus.EXECUTE_SUBMARINE_FINISHED;
import static org.apache.zeppelin.submarine.statemachine.SubmarineJobStatus.EXECUTE_SUBMARINE;
import static org.apache.zeppelin.submarine.statemachine.SubmarineJobStatus.UNKNOWN;
import static org.apache.zeppelin.submarine.statemachine.SubmarineStateMachineEvent.ToDashboard;
import static org.apache.zeppelin.submarine.statemachine.SubmarineStateMachineEvent.ToJobRun;
import static org.apache.zeppelin.submarine.statemachine.SubmarineStateMachineEvent.ToJobShow;
import static org.apache.zeppelin.submarine.statemachine.SubmarineStateMachineEvent.ToShowUsage;
import static org.apache.zeppelin.submarine.statemachine.SubmarineStateMachineState.DASHBOARD;
import static org.apache.zeppelin.submarine.statemachine.SubmarineStateMachineState.INITIALIZATION;
import static org.apache.zeppelin.submarine.statemachine.SubmarineStateMachineState.JOB_RUN;
import static org.apache.zeppelin.submarine.statemachine.SubmarineStateMachineState.JOB_SHOW;
import static org.apache.zeppelin.submarine.statemachine.SubmarineStateMachineState.SHOW_USAGE;
import static org.apache.zeppelin.submarine.utils.SubmarineUtils.unifyKey;

public class SubmarineJob {
  private Logger LOGGER = LoggerFactory.getLogger(SubmarineJob.class);

  private AtomicBoolean running = new AtomicBoolean(true);

  private YarnClient yarnClient = null;

  private SubmarineUI submarineUI = null;

  private Properties properties = null;

  private HDFSUtils hdfsUtils;

  private File pythonWorkDir = null;

  private String noteId = null;
  private String applicationId = null;
  private YarnApplicationState yarnApplicationState = null;
  private FinalApplicationStatus finalApplicationStatus = null;
  private long startTime = 0;
  private long launchTime = 0;
  private long finishTime = 0;
  private float progress = 0; // [0 ~ 100]
  private SubmarineJobStatus currentJobStatus = EXECUTE_SUBMARINE;

  // Submarine StateMachine
  SubmarineStateMachine submarineStateMachine = null;
  SubmarineStateMachineContext submarineSMC = null;

  private InterpreterContext intpContext = null;

  private Map<String, Object> mapSubmarineMenu = new HashMap<>();

  private static final String DIRECTORY_USER_HOME = "shell.working.directory.user.home";
  private final boolean isWindows = System.getProperty("os.name").startsWith("Windows");
  private final String shell = isWindows ? "cmd /c" : "bash -c";
  private static final String TIMEOUT_PROPERTY = "submarine.command.timeout.millisecond";
  private String defaultTimeout = "60000";

  public static final String SUBMARINE_JOBRUN_TF_JINJA
      = "jinja_templates/submarine-job-run-tf.jinja";
  public static final String SUBMARINE_COMMAND_JINJA
      = "jinja_templates/submarine-command.jinja";

  public SubmarineJob(InterpreterContext context, Properties properties) {
    this.intpContext = context;
    this.properties = properties;
    this.noteId = context.getNoteId();
    this.yarnClient = new YarnClient();
    this.hdfsUtils = new HDFSUtils("/", properties);
    this.submarineUI = new SubmarineUI(intpContext);

    // createSubmarineStateMachine(context);
  }

  private void createSubmarineStateMachine(InterpreterContext context) {
    try {
      submarineSMC = new SubmarineStateMachineContext(this);

      StateMachineBuilder<SubmarineStateMachine, SubmarineStateMachineState,
          SubmarineStateMachineEvent, SubmarineStateMachineContext> builder =
          StateMachineBuilderFactory.create(SubmarineStateMachine.class,
              SubmarineStateMachineState.class, SubmarineStateMachineEvent.class,
              SubmarineStateMachineContext.class);

      builder.onEntry(INITIALIZATION).callMethod("entryToInitialization");

      // INITIALIZATION -> Dashboard
      builder.externalTransitions().fromAmong(INITIALIZATION,
          JOB_RUN, JOB_SHOW, SHOW_USAGE)
          .to(DASHBOARD).on(ToDashboard).callMethod("fromAnyToAny");
      builder.onEntry(DASHBOARD).callMethod("entryToDashboard");

      // Dashboard -> JobRun
      builder.externalTransitions().fromAmong(INITIALIZATION,
          JOB_SHOW, SHOW_USAGE, DASHBOARD)
          .to(JOB_RUN).on(ToJobRun).callMethod("fromAnyToAny");
      builder.onEntry(JOB_RUN).callMethod("entryToJobRun");

      // Dashboard -> JobShow
      builder.externalTransitions().fromAmong(INITIALIZATION,
          JOB_RUN, SHOW_USAGE, DASHBOARD)
          .to(JOB_SHOW).on(ToJobShow).callMethod("fromAnyToAny");
      builder.onEntry(JOB_SHOW).callMethod("entryToJobShow");

      // Dashboard -> ShowUsage
      builder.externalTransitions().fromAmong(INITIALIZATION,
          JOB_RUN, JOB_SHOW, DASHBOARD)
          .to(SHOW_USAGE).on(ToShowUsage).callMethod("fromAnyToAny");
      builder.onEntry(SHOW_USAGE).callMethod("entryToShowUsage");

      submarineStateMachine = builder.newStateMachine(INITIALIZATION,
          StateMachineConfiguration.create().enableDebugMode(true));
      submarineStateMachine.start();
    } catch (Exception e) {
      e.printStackTrace();
      LOGGER.error("createSubmarineStateMachine: " + e.getMessage());
    }
  }

  public Properties getProperties() {
    return properties;
  }

  public HDFSUtils getHdfsUtils() {
    return hdfsUtils;
  }

  public SubmarineUI getSubmarineUI() {
    return submarineUI;
  }

  public void setPythonWorkDir(File pythonWorkDir) {
    this.pythonWorkDir = pythonWorkDir;
  }

  public void onDashboard() {
    submarineUI.createSubmarineUI(SubmarineCommand.DASHBOARD);
  }

  public void onJobRun() {
    // Check if it already exists
    Map<String, Object> mapYarnAppStatus = getJobStateByYarn();
    if (mapYarnAppStatus.size() == 0) {
      // Need to display the UI when the page is reloaded, don't create it in the thread
      submarineUI.createSubmarineUI(SubmarineCommand.JOB_RUN);
      jobRunThread();
    }
  }

  public void onShowUsage() {
    submarineUI.createSubmarineUI(SubmarineCommand.USAGE);
  }

  public void onJobShow() {
    // Check if it already exists
    Map<String, Object> mapYarnAppStatus = getJobStateByYarn();
    if (mapYarnAppStatus.size() > 0) {
      // Need to display the UI when the page is reloaded, don't create it in the thread
      submarineUI.createSubmarineUI(SubmarineCommand.JOB_SHOW);
      jobShowThread();
    }
  }

  public void onCleanRuntimeCache() {
    intpContext.getAngularObjectRegistry().removeAll(noteId, intpContext.getParagraphId());
    submarineUI.createSubmarineUI(SubmarineCommand.DASHBOARD);
  }

  // Note: The UI created in the thread does not display when the page is reloaded.
  public void jobShowThread() {
    new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          submarineUI.createLogHeadUI();
          setCurrentJobState(EXECUTE_SUBMARINE);
          submarineCommand(SubmarineCommand.JOB_SHOW);
        } catch (Exception e) {
          setCurrentJobState(EXECUTE_SUBMARINE_ERROR);
          submarineUI.outputLog("Exception", e.getMessage());
        } finally {

        }
      }
    }).start();
  }

  public void onJobCancle() {
    // Check if it already exists
    Map<String, Object> mapYarnAppStatus = getJobStateByYarn();
    if (mapYarnAppStatus.size() > 0) {
      jobCancelThread();
    }
  }

  // Note: The UI created in the thread does not display when the page is reloaded.
  public void jobCancelThread() {
    new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          submarineUI.createSubmarineUI(SubmarineCommand.JOB_CANCEL);
          submarineUI.createLogHeadUI();
          setCurrentJobState(EXECUTE_SUBMARINE);
          submarineCommand(SubmarineCommand.JOB_SHOW);
        } catch (Exception e) {
          setCurrentJobState(EXECUTE_SUBMARINE_ERROR);
          submarineUI.outputLog("Exception", e.getMessage());
        } finally {

        }
      }
    }).start();
  }

  public void jobRunThread() {
    new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          setCurrentJobState(EXECUTE_SUBMARINE);
          submarineUI.createLogHeadUI();

          String algorithmPath = properties.getProperty(
              SubmarineConstants.SUBMARINE_ALGORITHM_HDFS_PATH);
          if (!algorithmPath.startsWith("hdfs://")) {
            String message = "Algorithm file upload HDFS path, " +
                "Must be `hdfs://` prefix. now setting " + algorithmPath;
            submarineUI.outputLog("Configuration error", message);
            return;
          }

          String noteId = intpContext.getNoteId();
          String outputMsg = hdfsUtils.saveParagraphToFiles(noteId, intpContext.getNoteName(),
              pythonWorkDir == null ? "" : pythonWorkDir.getAbsolutePath(), properties);
          if (!StringUtils.isEmpty(outputMsg)) {
            submarineUI.outputLog("Save algorithm file", outputMsg);
          }
          showJobProgressBar(0.1f);

          HashMap jinjaParams = SubmarineUtils.propertiesToJinjaParams(properties, submarineUI,
              hdfsUtils, noteId, true);

          URL urlTemplate = Resources.getResource(SUBMARINE_JOBRUN_TF_JINJA);
          String template = Resources.toString(urlTemplate, Charsets.UTF_8);
          Jinjava jinjava = new Jinjava();
          String submarineCmd = jinjava.render(template, jinjaParams);

          LOGGER.info("Execute : " + submarineCmd);

          String jobName = SubmarineUtils.getJobName(noteId);
          StringBuffer sbLogs = new StringBuffer();
          sbLogs.append("Submarine submit job : " + jobName);
          sbLogs.append(submarineCmd);
          submarineUI.outputLog("Submarine submit command", sbLogs.toString());

          showJobProgressBar(1);

          CommandLine cmdLine = CommandLine.parse(shell);
          cmdLine.addArgument(submarineCmd, false);
          DefaultExecutor executor = new DefaultExecutor();
          StringBuffer sbLogOutput = new StringBuffer();
          executor.setStreamHandler(new PumpStreamHandler(new LogOutputStream() {
            @Override
            protected void processLine(String line, int level) {
              line = line.trim();
              if (!StringUtils.isEmpty(line)) {
                sbLogOutput.append(line + "\n");
                showJobProgressBar(0.1f);
              }
            }
          }));
          long timeout = Long.valueOf(properties.getProperty(TIMEOUT_PROPERTY,
              defaultTimeout));

          executor.setWatchdog(new ExecuteWatchdog(timeout));
          if (Boolean.valueOf(properties.getProperty(DIRECTORY_USER_HOME))) {
            executor.setWorkingDirectory(new File(System.getProperty("user.home")));
          }

          int exitVal = executor.execute(cmdLine);
          LOGGER.info("jobName {} return with exit value: {}", jobName, exitVal);
          submarineUI.outputLog(SubmarineCommand.JOB_RUN.getCommand(), sbLogOutput.toString());
          setCurrentJobState(EXECUTE_SUBMARINE_FINISHED);

          int loop = 100;
          while (loop-- > 0) {
            getJobStateByYarn();
            showJobProgressBar(0.1f);
            Thread.sleep(1000);
          }
        } catch (Exception e) {
          e.printStackTrace();
          setCurrentJobState(EXECUTE_SUBMARINE_ERROR);
          submarineUI.outputLog("Exception", e.getMessage());
        } finally {

        }
      }
    }).start();
  }

  // from state to state
  private void setCurrentJobState(SubmarineJobStatus toStatus) {
    switch (toStatus) {
      case EXECUTE_SUBMARINE:
        showJobProgressBar(0);
        break;
      case EXECUTE_SUBMARINE_FINISHED:
        break;
      case EXECUTE_SUBMARINE_ERROR:
        showJobProgressBar(0);
        break;
      default:
        LOGGER.error("unknown SubmarineJobStatus:" + currentJobStatus);
        break;
    }

    switch (currentJobStatus) {
      case EXECUTE_SUBMARINE:
        break;
      case EXECUTE_SUBMARINE_FINISHED:
        break;
      case EXECUTE_SUBMARINE_ERROR:
        break;
      default:
        LOGGER.error("unknown SubmarineJobStatus:" + currentJobStatus);
        break;
    }

    SubmarineUtils.setAngularObjectValue(intpContext,
        SubmarineConstants.JOB_STATUS, toStatus.getStatus());
    currentJobStatus = toStatus;
  }

  public Map<String, Object> getJobStateByYarn() {
    String jobName = SubmarineUtils.getJobName(noteId);
    Map<String, Object> mapStatus = yarnClient.getAppStatus(jobName);

    if (mapStatus.containsKey(SubmarineConstants.YARN_APPLICATION_ID)
        && mapStatus.containsKey(SubmarineConstants.YARN_APPLICATION_NAME)
        && mapStatus.containsKey(SubmarineConstants.YARN_APPLICATION_STATUS)) {
      String appId = mapStatus.get(SubmarineConstants.YARN_APPLICATION_ID).toString();
      String appName = mapStatus.get(SubmarineConstants.YARN_APPLICATION_NAME).toString();
      String appStatus = mapStatus.get(SubmarineConstants.YARN_APPLICATION_STATUS).toString();

      // create YARN UI link
      StringBuffer sbUrl = new StringBuffer();
      String yarnBaseUrl = properties.getProperty(SubmarineConstants.YARN_WEB_HTTP_ADDRESS, "");
      sbUrl.append(yarnBaseUrl).append("/ui2/#/yarn-app/").append(appId);
      sbUrl.append("/components?service=").append(appName);
      if (!mapSubmarineMenu.containsKey("Yarn log")) {
        Map<String, String> infos = new java.util.HashMap<>();
        infos.put("jobUrl", sbUrl.toString());
        infos.put("jobLabel", "Yarn log");
        infos.put("label", "Submarine WEB");
        infos.put("tooltip", "View in Submarine web UI");
        infos.put("noteId", noteId);
        infos.put("paraId", intpContext.getParagraphId());
        mapSubmarineMenu.put("Yarn log", infos);
        intpContext.getIntpEventClient().onParaInfosReceived(infos);
      }
      if (!mapSubmarineMenu.containsKey("Tensorboard")) {
        Map<String, String> infos2 = new java.util.HashMap<>();
        infos2.put("jobUrl", "http://192.168.0.1/tensorboard");
        infos2.put("jobLabel", "Tensorboard");
        infos2.put("label", "Submarine WEB");
        infos2.put("tooltip", "View in Submarine web UI");
        infos2.put("noteId", noteId);
        infos2.put("paraId", intpContext.getParagraphId());
        mapSubmarineMenu.put("Tensorboard", infos2);
        intpContext.getIntpEventClient().onParaInfosReceived(infos2);
      }

      SubmarineUtils.setAngularObjectValue(intpContext,
          SubmarineConstants.YARN_APPLICATION_ID, appId);

      SubmarineUtils.setAngularObjectValue(intpContext,
          SubmarineConstants.YARN_APPLICATION_STATUS, appStatus);

      SubmarineUtils.setAngularObjectValue(intpContext,
          SubmarineConstants.YARN_APPLICATION_URL, sbUrl.toString());

      SubmarineUtils.setAngularObjectValue(intpContext,
          SubmarineConstants.YARN_APP_TENSORFLOW_URL, "http://192.168.0.1/tensorboard");

      SubmarineUtils.setAngularObjectValue(intpContext,
          SubmarineConstants.JOB_PROGRESS, 0);

      SubmarineUtils.setAngularObjectValue(intpContext,
          SubmarineConstants.YARN_APP_EXECUTE_TIME, "2 hours ago");

      SubmarineJobStatus jobStatus = convertYarnState(appStatus);
      setCurrentJobState(jobStatus);
    }

    return mapStatus;
  }

  private SubmarineJobStatus convertYarnState(String appStatus) {
    switch (appStatus) {
      case "NEW":
        return SubmarineJobStatus.YARN_NEW;
      case "NEW_SAVING":
        return SubmarineJobStatus.YARN_NEW_SAVING;
      case "SUBMITTED":
        return SubmarineJobStatus.YARN_SUBMITTED;
      case "ACCEPTED":
        return SubmarineJobStatus.YARN_ACCEPTED;
      case "RUNNING":
        return SubmarineJobStatus.YARN_RUNNING;
      case "FINISHED":
        return SubmarineJobStatus.YARN_FINISHED;
      case "FAILED":
        return SubmarineJobStatus.YARN_FAILED;
      case "KILLED":
        return SubmarineJobStatus.YARN_KILLED;
    }

    return UNKNOWN;
  }

  private void showJobProgressBar(float increase) {
    if (increase == 0) {
      // init progress
      progress = 0;
    } else if (increase > 0 && increase < 100) {
      progress = progress + increase;
      if (progress >= 100) {
        progress = 99;
      }
    } else {
      // hide progress bar
      progress = 100;
    }

    SubmarineUtils.setAngularObjectValue(intpContext,
        SubmarineConstants.JOB_PROGRESS, Math.ceil(progress));
  }

  private void submarineCommand(SubmarineCommand command) {
    try {
      HashMap jinjaParams = SubmarineUtils.propertiesToJinjaParams(
          properties, submarineUI, hdfsUtils, noteId, false);
      String jobName = SubmarineUtils.getJobName(noteId);
      jinjaParams.put(unifyKey(SubmarineConstants.JOB_NAME), jobName);
      jinjaParams.put(unifyKey(SubmarineConstants.COMMAND_TYPE), command.getCommand());

      URL urlTemplate = Resources.getResource(SubmarineJob.SUBMARINE_COMMAND_JINJA);
      String template = Resources.toString(urlTemplate, Charsets.UTF_8);
      Jinjava jinjava = new Jinjava();
      String submarineCmd = jinjava.render(template, jinjaParams);

      LOGGER.info("Execute : " + submarineCmd);
      submarineUI.outputLog("Submarine submit command", submarineCmd);
      CommandLine cmdLine = CommandLine.parse(shell);
      cmdLine.addArgument(submarineCmd, false);

      DefaultExecutor executor = new DefaultExecutor();

      StringBuffer sbLogOutput = new StringBuffer();
      executor.setStreamHandler(new PumpStreamHandler(new LogOutputStream() {
        @Override
        protected void processLine(String line, int level) {
          sbLogOutput.append(line + "\n");
          showJobProgressBar(0.1f);
        }
      }));

      // executor.setStreamHandler(new PumpStreamHandler(interpreterOutput, interpreterOutput));
      long timeout = Long.valueOf(properties.getProperty(TIMEOUT_PROPERTY, defaultTimeout));

      executor.setWatchdog(new ExecuteWatchdog(timeout));
      if (Boolean.valueOf(properties.getProperty(DIRECTORY_USER_HOME))) {
        executor.setWorkingDirectory(new File(System.getProperty("user.home")));
      }

      int exitVal = executor.execute(cmdLine);
      LOGGER.info("jobName {} return with exit value: {}", jobName, exitVal);
      submarineUI.outputLog(command.getCommand(), sbLogOutput.toString());
      setCurrentJobState(EXECUTE_SUBMARINE_FINISHED);
      showJobProgressBar(100);
    } catch (IOException e) {
      setCurrentJobState(EXECUTE_SUBMARINE_ERROR);
      e.printStackTrace();
      submarineUI.outputLog("Exception", e.getMessage());
    }
  }

  public InterpreterContext getIntpContext() {
    return intpContext;
  }

  public void setIntpContext(InterpreterContext intpContext) {
    this.intpContext = intpContext;
    this.submarineUI = new SubmarineUI(intpContext);
  }
}

