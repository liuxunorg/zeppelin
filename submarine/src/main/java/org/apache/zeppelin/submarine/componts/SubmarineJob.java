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

package org.apache.zeppelin.submarine.componts;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.zeppelin.interpreter.InterpreterContext;
import org.apache.zeppelin.submarine.componts.thread.JobRunThread;
import org.apache.zeppelin.submarine.componts.thread.TensorboardRunThread;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.zeppelin.submarine.componts.SubmarineJobStatus.EXECUTE_SUBMARINE;

public class SubmarineJob extends Thread {
  // org/apache/hadoop/yarn/api/records/YarnApplicationState.class
  public enum YarnApplicationState {
    NEW,
    NEW_SAVING,
    SUBMITTED,
    ACCEPTED,
    RUNNING,
    FINISHED,
    FAILED,
    KILLED;
  }

  // org/apache/hadoop/yarn/api/records/FinalApplicationStatus.class
  public enum FinalApplicationStatus {
    UNDEFINED,
    SUCCEEDED,
    FAILED,
    KILLED,
    ENDED;
  }

  private Logger LOGGER = LoggerFactory.getLogger(SubmarineJob.class);

  private AtomicBoolean running = new AtomicBoolean(true);

  private static final long SYNC_SUBMARINE_RUNTIME_CYCLE = 3000;

  // Avoid repeated calls by users
  public static final long SUBMARIN_RUN_WAIT_TIME = 10;
  private AtomicLong jobRunWaitTime = new AtomicLong(0);
  private AtomicLong tensorboardRunWaitTime = new AtomicLong(0);

  private YarnClient yarnClient = null;

  private SubmarineUI submarineUI = null;

  private Properties properties = null;

  private HdfsClient hdfsClient;

  private File pythonWorkDir = null;

  private String noteId = null;
  private String noteName = null;
  private String userName = null;
  private String applicationId = null;
  private YarnApplicationState yarnApplicationState = null;
  private FinalApplicationStatus finalApplicationStatus = null;
  private long startTime = 0;
  private long launchTime = 0;
  private long finishTime = 0;
  private float progress = 0; // [0 ~ 100]
  private SubmarineJobStatus currentJobStatus = EXECUTE_SUBMARINE;

  private InterpreterContext intpContext = null;

  private Map<String, Object> mapSubmarineMenu = new HashMap<>();

  public static final String DIRECTORY_USER_HOME = "shell.working.directory.userName.home";
  private static final boolean isWindows = System.getProperty("os.name").startsWith("Windows");
  public static final String shell = isWindows ? "cmd /c" : "bash -c";
  public static final String TIMEOUT_PROPERTY = "submarine.command.timeout.millisecond";
  public static final String defaultTimeout = "60000";

  public static final String SUBMARINE_JOBRUN_TF_JINJA
      = "jinja_templates/submarine-job-run-tf.jinja";
  public static final String SUBMARINE_COMMAND_JINJA
      = "jinja_templates/submarine-command.jinja";
  public static final String SUBMARINE_TENSORBOARD_JINJA
      = "jinja_templates/submarine-tensorboard.jinja";

  public SubmarineJob(InterpreterContext context, Properties properties) {
    this.intpContext = context;
    this.properties = properties;
    this.noteId = context.getNoteId();
    this.noteName = context.getNoteName();
    this.userName = context.getAuthenticationInfo().getUser();
    this.yarnClient = new YarnClient(properties);
    this.hdfsClient = new HdfsClient(properties);
    this.submarineUI = new SubmarineUI(intpContext);
    this.start();
  }

  // 1. Synchronize submarine runtime state
  // 2. Update jobRunWaitTime & tensorboardRunWaitTime
  @Override
  public void run() {
    while (running.get()) {
      String jobName = SubmarineUtils.getJobName(userName, noteId);
      Map<String, Object> jobState = getJobStateByYarn(jobName);

      getTensorboardStatus();

      //
      if (jobRunWaitTime.get() > 0) {
        jobRunWaitTime.decrementAndGet();
      }
      if (tensorboardRunWaitTime.get() > 0) {
        tensorboardRunWaitTime.decrementAndGet();
      }
      try {
        Thread.sleep(SYNC_SUBMARINE_RUNTIME_CYCLE);
      } catch (InterruptedException e) {
        LOGGER.error(e.getMessage(), e);
      }
    }
  }

  public String getUserTensorboardPath() {
    String tfCheckpointPath = properties.getProperty(SubmarineConstants.TF_CHECKPOINT_PATH, "");
    return tfCheckpointPath;
  }

  public String getJobDefaultCheckpointPath() {
    String userTensorboardPath = getUserTensorboardPath();
    return userTensorboardPath + "/" + noteId;
  }

  public void cleanJobDefaultCheckpointPath() {
    String jobCheckpointPath = getJobDefaultCheckpointPath();
    Path notePath = new Path(jobCheckpointPath);
    if (notePath.depth() <= 3) {
      submarineUI.outputLog("ERROR", "Checkpoint path depth must be greater than 3");
      return;
    }
    try {
      String message = "Clean up the checkpoint directory: " + jobCheckpointPath;
      submarineUI.outputLog("", message);
      hdfsClient.delete(notePath);
    } catch (IOException e) {
      LOGGER.error(e.getMessage(), e);
    }
  }

  public Properties getProperties() {
    return properties;
  }

  public HdfsClient getHdfsClient() {
    return hdfsClient;
  }

  public SubmarineUI getSubmarineUI() {
    return submarineUI;
  }

  public void setPythonWorkDir(File pythonWorkDir) {
    this.pythonWorkDir = pythonWorkDir;
  }

  public File getPythonWorkDir() {
    return this.pythonWorkDir;
  }

  public String getNoteName() {
    return noteName;
  }

  public void onDashboard() {
    submarineUI.createSubmarineUI(SubmarineCommand.DASHBOARD);
  }

  public void setTensorboardRunWaitTime(long time) {
    tensorboardRunWaitTime.set(time);
  }

  public void setJobRunWaitTime(long time) {
    jobRunWaitTime.set(time);
  }

  public void runJob() {
    // Need to display the UI when the page is reloaded, don't create it in the thread
    submarineUI.createSubmarineUI(SubmarineCommand.JOB_RUN);
    submarineUI.createLogHeadUI();

    // Check if job already exists
    String jobName = SubmarineUtils.getJobName(userName, noteId);
    Map<String, Object> mapYarnAppStatus = getJobStateByYarn(jobName);
    if (mapYarnAppStatus.size() == 0) {
      long waitTime = jobRunWaitTime.get();
      if (waitTime > 0) {
        String message = "Avoid repeated calls run Job by the " + userName +
            "please wait " + SYNC_SUBMARINE_RUNTIME_CYCLE * waitTime + " seconds.";
        LOGGER.info(message);
        submarineUI.outputLog("Warn", message);
        return;
      }

      JobRunThread jobRunThread = new JobRunThread(this);
      jobRunThread.start();
    } else {
      submarineUI.outputLog("", "JOB " + jobName + " Already running.");
    }
  }

  public void deleteJob(String serviceName) {
    submarineUI.createSubmarineUI(SubmarineCommand.JOB_STOP);
    yarnClient.deleteService(serviceName);
  }

  public void runTensorBoard() {
    submarineUI.createSubmarineUI(SubmarineCommand.TENSORBOARD_RUN);
    submarineUI.createLogHeadUI();

    boolean tensorboardExist = getTensorboardStatus();
    if (false == tensorboardExist) {
      long waitTime = tensorboardRunWaitTime.get();
      if (waitTime > 0) {
        String message = "Avoid repeated calls run Tensorboard by the " + userName + ", " +
            "please wait " + SYNC_SUBMARINE_RUNTIME_CYCLE * waitTime + " seconds.";
        LOGGER.info(message);
        submarineUI.outputLog("Warn", message);
        return;
      }

      TensorboardRunThread tensorboardRunThread = new TensorboardRunThread(this);
      tensorboardRunThread.start();
    }
  }

  // Check if tensorboard already exists
  public boolean getTensorboardStatus() {
    String enableTensorboard = properties.getProperty(
        SubmarineConstants.TF_TENSORBOARD_ENABLE, "false");
    boolean tensorboardExist = false;
    if (StringUtils.equals(enableTensorboard, "true")) {
      String tensorboardName = SubmarineUtils.getTensorboardName(userName);
      List<Map<String, Object>> listExportPorts = yarnClient.getAppExportPorts(tensorboardName);
      for (Map<String, Object> exportPorts : listExportPorts) {
        if (exportPorts.containsKey(YarnClient.HOST_IP)
            && exportPorts.containsKey(YarnClient.HOST_PORT)
            && exportPorts.containsKey(YarnClient.CONTAINER_PORT)) {
          String intpAppHostIp = (String) exportPorts.get(YarnClient.HOST_IP);
          String intpAppHostPort = (String) exportPorts.get(YarnClient.HOST_PORT);
          String intpAppContainerPort = (String) exportPorts.get(YarnClient.CONTAINER_PORT);
          if (StringUtils.equals("6006", intpAppContainerPort)) {
            tensorboardExist = true;
            LOGGER.info("Detection tensorboard Container hostIp:{}, hostPort:{}, containerPort:{}.",
                intpAppHostIp, intpAppHostPort, intpAppContainerPort);

            // show tensorboard menu & link button
            SubmarineUtils.setAgulObjValue(intpContext,
                SubmarineConstants.YARN_TENSORBOARD_URL,
                "http://" + intpAppHostIp + ":" + intpAppHostPort);
            break;
          }
        }
      }

      if (false == tensorboardExist) {
        SubmarineUtils.removeAgulObjValue(intpContext, SubmarineConstants.YARN_TENSORBOARD_URL);
      }
    }

    return tensorboardExist;
  }

  public void showUsage() {
    submarineUI.createSubmarineUI(SubmarineCommand.USAGE);
  }

  public void cleanRuntimeCache() {
    intpContext.getAngularObjectRegistry().removeAll(noteId, intpContext.getParagraphId());
    submarineUI.createSubmarineUI(SubmarineCommand.DASHBOARD);
  }

  public String getNoteId() {
    return noteId;
  }

  public String getUserName() {
    return this.userName;
  }

  // from state to state
  public void setCurrentJobState(SubmarineJobStatus toStatus) {
    SubmarineUtils.setAgulObjValue(intpContext, SubmarineConstants.JOB_STATUS,
        toStatus.getStatus());
    currentJobStatus = toStatus;
  }

  public Map<String, Object> getJobStateByYarn(String jobName) {
    Map<String, Object> mapStatus = yarnClient.getAppServices(jobName);

    if (mapStatus.containsKey(SubmarineConstants.YARN_APPLICATION_ID)
        && mapStatus.containsKey(SubmarineConstants.YARN_APPLICATION_NAME)) {
      String appId = mapStatus.get(SubmarineConstants.YARN_APPLICATION_ID).toString();
      String appName = mapStatus.get(SubmarineConstants.YARN_APPLICATION_NAME).toString();

      String state = "", finalStatus = "";
      Map<String, Object> clusterApps = yarnClient.getClusterApps(appId);
      if (clusterApps.containsKey("state")) {
        state = clusterApps.get("state").toString();
        SubmarineUtils.setAgulObjValue(intpContext,
            SubmarineConstants.YARN_APPLICATION_STATUS, state);
      }
      if (clusterApps.containsKey("finalStatus")) {
        finalStatus = clusterApps.get("finalStatus").toString();
        SubmarineUtils.setAgulObjValue(intpContext,
            SubmarineConstants.YARN_APPLICATION_FINAL_STATUS, finalStatus);
      }
      SubmarineJobStatus jobStatus = convertYarnState(state, finalStatus);
      setCurrentJobState(jobStatus);
      try {
        if (clusterApps.containsKey("startedTime")) {
          String startedTime = clusterApps.get("startedTime").toString();
          long lStartedTime = Long.parseLong(startedTime);
          if (lStartedTime > 0) {
            Date startedDate = new Date(lStartedTime);
            SubmarineUtils.setAgulObjValue(intpContext,
                SubmarineConstants.YARN_APP_STARTED_TIME, startedDate.toString());
          }
        }
        if (clusterApps.containsKey("launchTime")) {
          String launchTime = clusterApps.get("launchTime").toString();
          long lLaunchTime = Long.parseLong(launchTime);
          if (lLaunchTime > 0) {
            Date launchDate = new Date(lLaunchTime);
            SubmarineUtils.setAgulObjValue(intpContext,
                SubmarineConstants.YARN_APP_LAUNCH_TIME, launchDate.toString());
          }
        }
        if (clusterApps.containsKey("finishedTime")) {
          String finishedTime = clusterApps.get("finishedTime").toString();
          long lFinishedTime = Long.parseLong(finishedTime);
          if (lFinishedTime > 0) {
            Date finishedDate = new Date(lFinishedTime);
            SubmarineUtils.setAgulObjValue(intpContext,
                SubmarineConstants.YARN_APP_FINISHED_TIME, finishedDate.toString());
          }
        }
        if (clusterApps.containsKey("elapsedTime")) {
          String elapsedTime = clusterApps.get("elapsedTime").toString();
          long lElapsedTime = Long.parseLong(elapsedTime);
          if (lElapsedTime > 0) {
            String finishedDate = org.apache.hadoop.util.StringUtils.formatTime(lElapsedTime);
            SubmarineUtils.setAgulObjValue(intpContext,
                SubmarineConstants.YARN_APP_ELAPSED_TIME, finishedDate);
          }
        }
      } catch (NumberFormatException e) {
        LOGGER.error(e.getMessage());
      }

      // create YARN UI link
      StringBuffer sbUrl = new StringBuffer();
      String yarnBaseUrl = properties.getProperty(SubmarineConstants.YARN_WEB_HTTP_ADDRESS, "");
      sbUrl.append(yarnBaseUrl).append("/ui2/#/yarn-app/").append(appId);
      sbUrl.append("/components?service=").append(appName);

      SubmarineUtils.setAgulObjValue(intpContext, SubmarineConstants.YARN_APPLICATION_ID, appId);

      SubmarineUtils.setAgulObjValue(intpContext, SubmarineConstants.YARN_APPLICATION_URL,
          sbUrl.toString());

      SubmarineUtils.setAgulObjValue(intpContext, SubmarineConstants.JOB_PROGRESS, 0);
    } else {
      SubmarineUtils.removeAgulObjValue(intpContext, SubmarineConstants.YARN_APPLICATION_ID);
      SubmarineUtils.removeAgulObjValue(intpContext, SubmarineConstants.YARN_APPLICATION_STATUS);
      SubmarineUtils.removeAgulObjValue(intpContext, SubmarineConstants.YARN_APPLICATION_URL);
      SubmarineUtils.removeAgulObjValue(intpContext, SubmarineConstants.YARN_APP_STARTED_TIME);
      SubmarineUtils.removeAgulObjValue(intpContext, SubmarineConstants.YARN_APP_LAUNCH_TIME);
      SubmarineUtils.removeAgulObjValue(intpContext, SubmarineConstants.YARN_APP_FINISHED_TIME);
      SubmarineUtils.removeAgulObjValue(intpContext, SubmarineConstants.YARN_APP_ELAPSED_TIME);
      if (jobRunWaitTime.get() <= 0) {
        // Not wait job run
        SubmarineUtils.removeAgulObjValue(intpContext, SubmarineConstants.JOB_STATUS);
      }
    }

    return mapStatus;
  }

  private SubmarineJobStatus convertYarnState(String status, String finalStatus) {
    SubmarineJobStatus submarineJobStatus = SubmarineJobStatus.UNKNOWN;
    switch (status) {
      case "NEW":
        submarineJobStatus = SubmarineJobStatus.YARN_NEW;
        break;
      case "NEW_SAVING":
        submarineJobStatus = SubmarineJobStatus.YARN_NEW_SAVING;
        break;
      case "SUBMITTED":
        submarineJobStatus = SubmarineJobStatus.YARN_SUBMITTED;
        break;
      case "ACCEPTED":
        submarineJobStatus = SubmarineJobStatus.YARN_ACCEPTED;
        break;
      case "RUNNING":
        submarineJobStatus = SubmarineJobStatus.YARN_RUNNING;
        break;
      case "FINISHED":
        submarineJobStatus = SubmarineJobStatus.YARN_FINISHED;
        break;
      case "FAILED":
        submarineJobStatus = SubmarineJobStatus.YARN_FAILED;
        break;
      case "KILLED":
        submarineJobStatus = SubmarineJobStatus.YARN_KILLED;
        break;
      case "STOPPED":
        submarineJobStatus = SubmarineJobStatus.YARN_STOPPED;
    }
    switch (finalStatus) {
      case "NEW":
        submarineJobStatus = SubmarineJobStatus.YARN_NEW;
        break;
      case "NEW_SAVING":
        submarineJobStatus = SubmarineJobStatus.YARN_NEW_SAVING;
        break;
      case "SUBMITTED":
        submarineJobStatus = SubmarineJobStatus.YARN_SUBMITTED;
        break;
      case "ACCEPTED":
        submarineJobStatus = SubmarineJobStatus.YARN_ACCEPTED;
        break;
      case "RUNNING":
        submarineJobStatus = SubmarineJobStatus.YARN_RUNNING;
        break;
      case "FINISHED":
        submarineJobStatus = SubmarineJobStatus.YARN_FINISHED;
        break;
      case "FAILED":
        submarineJobStatus = SubmarineJobStatus.YARN_FAILED;
        break;
      case "KILLED":
        submarineJobStatus = SubmarineJobStatus.YARN_KILLED;
        break;
      case "STOPPED":
        submarineJobStatus = SubmarineJobStatus.YARN_STOPPED;
        break;
      default: // UNDEFINED
        break;
    }

    return submarineJobStatus;
  }

  public void showJobProgressBar(float increase) {
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

    SubmarineUtils.setAgulObjValue(intpContext,
        SubmarineConstants.JOB_PROGRESS, Math.ceil(progress));
  }

  public InterpreterContext getIntpContext() {
    return intpContext;
  }

  public void setIntpContext(InterpreterContext intpContext) {
    this.intpContext = intpContext;
    this.submarineUI = new SubmarineUI(intpContext);
  }
}
