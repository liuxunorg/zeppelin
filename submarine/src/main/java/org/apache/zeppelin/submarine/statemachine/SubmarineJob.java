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
import org.apache.commons.exec.PumpStreamHandler;
import org.apache.commons.io.Charsets;
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
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.zeppelin.submarine.statemachine.SubmarineStateMachineEvent.ToDashboard;
import static org.apache.zeppelin.submarine.statemachine.SubmarineStateMachineEvent.ToJobRun;
import static org.apache.zeppelin.submarine.statemachine.SubmarineStateMachineEvent.ToJobShow;
import static org.apache.zeppelin.submarine.statemachine.SubmarineStateMachineEvent.ToShowUsage;
import static org.apache.zeppelin.submarine.statemachine.SubmarineStateMachineState.DASHBOARD;
import static org.apache.zeppelin.submarine.statemachine.SubmarineStateMachineState.INITIALIZATION;
import static org.apache.zeppelin.submarine.statemachine.SubmarineStateMachineState.JOB_RUN;
import static org.apache.zeppelin.submarine.statemachine.SubmarineStateMachineState.JOB_SHOW;
import static org.apache.zeppelin.submarine.statemachine.SubmarineStateMachineState.SHOW_USAGE;

public class SubmarineJob {
  private Logger LOGGER = LoggerFactory.getLogger(SubmarineJob.class);

  private AtomicBoolean running = new AtomicBoolean(true);

  private YarnClient yarnClient = null;

  private SubmarineUI submarineUI = null;

  private Properties properties = null;

  private HDFSUtils hdfsUtils;

  private File pythonWorkDir = null;

  private String nodeId = null;
  private String applicationId = null;
  private YarnApplicationState yarnApplicationState = null;
  private FinalApplicationStatus finalApplicationStatus = null;
  private long startTime = 0;
  private long launchTime = 0;
  private long finishTime = 0;
  private int progress = 0; // [0 ~ 100]
  private SubmarineNoteStatus noteStatus = SubmarineNoteStatus.READY;

  // Submarine StateMachine
  SubmarineStateMachine submarineStateMachine = null;
  SubmarineStateMachineContext submarineSMC = null;

  private InterpreterContext intpContext = null;

  private static final String DIRECTORY_USER_HOME = "shell.working.directory.user.home";
  private final boolean isWindows = System.getProperty("os.name").startsWith("Windows");
  private final String shell = isWindows ? "cmd /c" : "bash -c";
  private static final String TIMEOUT_PROPERTY = "submarine.command.timeout.millisecond";
  private String defaultTimeoutProperty = "60000";

  public static final String SUBMARINE_JOBRUN_TF_JINJA
      = "jinja_templates/submarine-job-run-tf.jinja";
  public static final String SUBMARINE_COMMAND_JINJA
      = "jinja_templates/submarine-command.jinja";

  public SubmarineJob(InterpreterContext context, Properties properties) {
    this.intpContext = context;
    this.properties = properties;
    this.nodeId = context.getNoteId();
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

  public String getNoteId() {
    return intpContext.getNoteId();
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

  public void onJobRun(boolean active) {
    submarineUI.createSubmarineUI(SubmarineCommand.JOB_RUN);

    if (active) {
      jobRun();
      //jobStop();
    }
  }

  public void onShowUsage() {
    submarineUI.createSubmarineUI(SubmarineCommand.USAGE);
  }

  public void onJobShow(boolean active) {
    submarineUI.createSubmarineUI(SubmarineCommand.JOB_SHOW);
    if (active) {
      submarineUI.createLogHeadUI();
    }
  }

  public void jobRun() {
    new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          submarineUI.createLogHeadUI();
          Thread.sleep(3000);
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
          submarineUI.outputLog("Save algorithm file", outputMsg);

          String jobName = SubmarineUtils.getJobName(noteId);
          HashMap jinjaParams = SubmarineUtils.propertiesToJinjaParams(properties, submarineUI,
              hdfsUtils, jobName, true);

          URL urlTemplate = Resources.getResource(SUBMARINE_JOBRUN_TF_JINJA);
          String template = Resources.toString(urlTemplate, Charsets.UTF_8);
          Jinjava jinjava = new Jinjava();
          String submarineCmd = jinjava.render(template, jinjaParams);

          LOGGER.info("Execute : " + submarineCmd);

          StringBuffer sbLogs = new StringBuffer();
          sbLogs.append("Submarine submit job : " + jobName);
          sbLogs.append(submarineCmd);
          submarineUI.outputLog("Submarine submit command", sbLogs.toString());

          CommandLine cmdLine = CommandLine.parse(shell);
          cmdLine.addArgument(submarineCmd, false);
          DefaultExecutor executor = new DefaultExecutor();
          executor.setStreamHandler(new PumpStreamHandler(intpContext.out, intpContext.out));
          long timeout = Long.valueOf(properties.getProperty(TIMEOUT_PROPERTY,
              defaultTimeoutProperty));

          executor.setWatchdog(new ExecuteWatchdog(timeout));
          if (Boolean.valueOf(properties.getProperty(DIRECTORY_USER_HOME))) {
            executor.setWorkingDirectory(new File(System.getProperty("user.home")));
          }

          int exitVal = executor.execute(cmdLine);
          LOGGER.info("jobName {} return with exit value: {}", jobName, exitVal);
        } catch (IOException e) {
          submarineUI.outputLog("Exception", e.getMessage());
        } catch (RuntimeException e) {
          submarineUI.outputLog("Exception", e.getMessage());
        } catch (Exception e) {
          submarineUI.outputLog("Exception", e.getMessage());
        }
      }
    }).start();
  }

  public void jobStop() {
    try {
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
      submarineUI.outputLog("Save algorithm file", outputMsg);

      String jobName = SubmarineUtils.getJobName(noteId);
      HashMap jinjaParams = SubmarineUtils.propertiesToJinjaParams(properties, submarineUI,
          hdfsUtils, jobName, true);

      URL urlTemplate = Resources.getResource(SUBMARINE_JOBRUN_TF_JINJA);
      String template = Resources.toString(urlTemplate, Charsets.UTF_8);
      Jinjava jinjava = new Jinjava();
      String submarineCmd = jinjava.render(template, jinjaParams);

      LOGGER.info("Execute : " + submarineCmd);

      StringBuffer sbLogs = new StringBuffer();
      sbLogs.append("Submarine submit job : " + jobName);
      sbLogs.append(submarineCmd);
      submarineUI.outputLog("Submarine submit command", sbLogs.toString());

      CommandLine cmdLine = CommandLine.parse(shell);
      cmdLine.addArgument(submarineCmd, false);
      DefaultExecutor executor = new DefaultExecutor();
      executor.setStreamHandler(new PumpStreamHandler(intpContext.out, intpContext.out));
      long timeout = Long.valueOf(properties.getProperty(TIMEOUT_PROPERTY,
          defaultTimeoutProperty));

      executor.setWatchdog(new ExecuteWatchdog(timeout));
      if (Boolean.valueOf(properties.getProperty(DIRECTORY_USER_HOME))) {
        executor.setWorkingDirectory(new File(System.getProperty("user.home")));
      }

      int exitVal = executor.execute(cmdLine);
      LOGGER.info("jobName {} return with exit value: {}", jobName, exitVal);
    } catch (IOException e) {
      submarineUI.outputLog("Exception", e.getMessage());
    } catch (RuntimeException e) {
      submarineUI.outputLog("Exception", e.getMessage());
    } catch (Exception e) {
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

