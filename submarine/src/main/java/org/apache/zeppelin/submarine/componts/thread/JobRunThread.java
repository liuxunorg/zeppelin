package org.apache.zeppelin.submarine.componts.thread;

import com.google.common.io.Resources;
import com.hubspot.jinjava.Jinjava;
import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecuteResultHandler;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.ExecuteException;
import org.apache.commons.exec.ExecuteWatchdog;
import org.apache.commons.exec.LogOutputStream;
import org.apache.commons.exec.PumpStreamHandler;
import org.apache.commons.io.Charsets;
import org.apache.commons.lang.StringUtils;
import org.apache.zeppelin.interpreter.InterpreterContext;
import org.apache.zeppelin.submarine.componts.HdfsClient;
import org.apache.zeppelin.submarine.componts.SubmarineConstants;
import org.apache.zeppelin.submarine.componts.SubmarineJob;
import org.apache.zeppelin.submarine.componts.SubmarineUI;
import org.apache.zeppelin.submarine.componts.SubmarineUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.URL;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.zeppelin.submarine.componts.SubmarineJobStatus.EXECUTE_SUBMARINE;
import static org.apache.zeppelin.submarine.componts.SubmarineJobStatus.EXECUTE_SUBMARINE_ERROR;
import static org.apache.zeppelin.submarine.componts.SubmarineJobStatus.EXECUTE_SUBMARINE_FINISHED;

public class JobRunThread extends Thread {
  private Logger LOGGER = LoggerFactory.getLogger(JobRunThread.class);

  private SubmarineJob submarineJob;

  public JobRunThread(SubmarineJob submarineJob) {
    this.submarineJob = submarineJob;
  }

  public void run() {
    SubmarineUI submarineUI = submarineJob.getSubmarineUI();
    Properties properties = submarineJob.getProperties();
    InterpreterContext intpContext = submarineJob.getIntpContext();
    HdfsClient hdfsClient = submarineJob.getHdfsClient();
    File pythonWorkDir = submarineJob.getPythonWorkDir();

    try {
      submarineJob.setCurrentJobState(EXECUTE_SUBMARINE);

      String algorithmPath = properties.getProperty(
          SubmarineConstants.SUBMARINE_ALGORITHM_HDFS_PATH);
      if (!algorithmPath.startsWith("hdfs://")) {
        String message = "Algorithm file upload HDFS path, " +
            "Must be `hdfs://` prefix. now setting " + algorithmPath;
        submarineUI.outputLog("Configuration error", message);
        return;
      }


      String noteId = intpContext.getNoteId();
      String noteName = intpContext.getNoteName();
      String userName = intpContext.getAuthenticationInfo().getUser();

      String noteJson = intpContext.getIntpEventClient().getNoteFromServer(
          noteId, intpContext.getAuthenticationInfo(), true);

      String outputMsg = hdfsClient.saveParagraphToFiles(noteId, noteJson,
          pythonWorkDir == null ? "" : pythonWorkDir.getAbsolutePath(), properties);
      if (!StringUtils.isEmpty(outputMsg)) {
        submarineUI.outputLog("Save algorithm file", outputMsg);
      }
      submarineJob.showJobProgressBar(0.1f);

      HashMap jinjaParams = SubmarineUtils.propertiesToJinjaParams(
          properties, submarineJob, true);

      URL urlTemplate = Resources.getResource(SubmarineJob.SUBMARINE_JOBRUN_TF_JINJA);
      String template = Resources.toString(urlTemplate, Charsets.UTF_8);
      Jinjava jinjava = new Jinjava();
      String submarineCmd = jinjava.render(template, jinjaParams);
      // If the first line is a newline, delete the newline
      int firstLineIsNewline = submarineCmd.indexOf("\n");
      if (firstLineIsNewline == 0) {
        submarineCmd = submarineCmd.replaceFirst("\n", "");
      }
      String jobName = SubmarineUtils.getJobName(userName, noteId);
      StringBuffer sbLogs = new StringBuffer(submarineCmd);
      submarineUI.outputLog("Submarine submit command", sbLogs.toString());

      submarineJob.showJobProgressBar(1);

      long timeout = Long.valueOf(properties.getProperty(SubmarineJob.TIMEOUT_PROPERTY,
          SubmarineJob.defaultTimeout));
      CommandLine cmdLine = CommandLine.parse(SubmarineJob.shell);
      cmdLine.addArgument(submarineCmd, false);
      DefaultExecutor executor = new DefaultExecutor();
      ExecuteWatchdog watchDog = new ExecuteWatchdog(timeout);
      executor.setWatchdog(watchDog);
      StringBuffer sbLogOutput = new StringBuffer();
      executor.setStreamHandler(new PumpStreamHandler(new LogOutputStream() {
        @Override
        protected void processLine(String line, int level) {
          line = line.trim();
          if (!StringUtils.isEmpty(line)) {
            sbLogOutput.append(line + "\n");
            submarineJob.showJobProgressBar(0.1f);
          }
        }
      }));

      if (Boolean.valueOf(properties.getProperty(SubmarineJob.DIRECTORY_USER_HOME))) {
        executor.setWorkingDirectory(new File(System.getProperty("user.home")));
      }

      Map<String, String> env = new HashMap<>();
      String launchMode = (String) jinjaParams.get(SubmarineConstants.INTERPRETER_LAUNCH_MODE);
      if (StringUtils.equals(launchMode, "yarn")) {
        // Set environment variables in the container
        String javaHome, hadoopHome, hadoopConf;
        javaHome = (String) jinjaParams.get(SubmarineConstants.DOCKER_JAVA_HOME);
        hadoopHome = (String) jinjaParams.get(SubmarineConstants.DOCKER_HADOOP_HDFS_HOME);
        hadoopConf = (String) jinjaParams.get(SubmarineConstants.SUBMARINE_HADOOP_CONF_DIR);
        env.put("JAVA_HOME", javaHome);
        env.put("HADOOP_HOME", hadoopHome);
        env.put("HADOOP_HDFS_HOME", hadoopHome);
        env.put("HADOOP_CONF_DIR", hadoopConf);
        env.put("YARN_CONF_DIR", hadoopConf);
        env.put("CLASSPATH", "`$HADOOP_HDFS_HOME/bin/hadoop classpath --glob`");
      }

      LOGGER.info("Execute EVN: {}, Command: {} ", env.toString(), submarineCmd);

      AtomicBoolean running = new AtomicBoolean(true);
      executor.execute(cmdLine, env, new DefaultExecuteResultHandler() {
        @Override
        public void onProcessComplete(int exitValue) {
          String message = String.format(
              "jobName %s ProcessComplete exit value is : %d", jobName, exitValue);
          LOGGER.info(message);
          submarineUI.outputLog("JOR RUN COMPLETE", message);
          running.set(false);
          submarineJob.setCurrentJobState(EXECUTE_SUBMARINE_FINISHED);
        }
        @Override
        public void onProcessFailed(ExecuteException e) {
          String message = String.format(
              "jobName %s ProcessFailed exit value is : %d, exception is : %s",
              jobName, e.getExitValue(), e.getMessage());
          LOGGER.error(message);
          submarineUI.outputLog("JOR RUN FAILED", message);
          running.set(false);
          submarineJob.setCurrentJobState(EXECUTE_SUBMARINE_ERROR);
        }
      });
      Date nowDate = new Date();
      Date checkDate = new Date();
      while (((checkDate.getTime() - nowDate.getTime()) < timeout) && running.get()) {
        Thread.sleep(1000);
      }
      if (watchDog.isWatching()) {
        watchDog.destroyProcess();
        Thread.sleep(1000);
      }
      if (watchDog.isWatching()) {
        watchDog.killedProcess();
      }
      submarineJob.setTensorboardRunWaitTime(SubmarineJob.SUBMARIN_RUN_WAIT_TIME);
    } catch (Exception e) {
      LOGGER.error(e.getMessage(), e);
      submarineJob.setCurrentJobState(EXECUTE_SUBMARINE_ERROR);
      submarineUI.outputLog("Exception", e.getMessage());
    }
  }
}
