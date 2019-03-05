package org.apache.zeppelin.submarine.job.thread;

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
import org.apache.zeppelin.submarine.hadoop.HdfsClient;
import org.apache.zeppelin.submarine.componts.SubmarineConstants;
import org.apache.zeppelin.submarine.componts.SubmarineUI;
import org.apache.zeppelin.submarine.componts.SubmarineUtils;
import org.apache.zeppelin.submarine.job.SubmarineJob;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.zeppelin.submarine.job.SubmarineJobStatus.EXECUTE_SUBMARINE;
import static org.apache.zeppelin.submarine.job.SubmarineJobStatus.EXECUTE_SUBMARINE_ERROR;
import static org.apache.zeppelin.submarine.job.SubmarineJobStatus.EXECUTE_SUBMARINE_FINISHED;

public class JobRunThread extends Thread {
  private Logger LOGGER = LoggerFactory.getLogger(JobRunThread.class);

  private SubmarineJob submarineJob;

  private AtomicBoolean running = new AtomicBoolean(false);

  public JobRunThread(SubmarineJob submarineJob) {
    this.submarineJob = submarineJob;
  }

  public void run() {
    SubmarineUI submarineUI = submarineJob.getSubmarineUI();
    InterpreterContext intpContext = submarineJob.getIntpContext();
    String noteId = intpContext.getNoteId();
    String userName = intpContext.getAuthenticationInfo().getUser();
    String jobName = SubmarineUtils.getJobName(userName, noteId);

    if (true == running.get()) {
      String message = String.format("Job %s already running.", jobName);
      submarineUI.outputLog("WARN", message);
      LOGGER.warn(message);
      return;
    }
    running.set(true);

    Properties properties = submarineJob.getProperties();
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

      String noteJson = intpContext.getIntpEventClient().getNoteFromServer(
          noteId, intpContext.getAuthenticationInfo(), true);
      String outputMsg = hdfsClient.saveParagraphToFiles(noteId, noteJson,
          pythonWorkDir == null ? "" : pythonWorkDir.getAbsolutePath(), properties);
      if (!StringUtils.isEmpty(outputMsg)) {
        submarineUI.outputLog("Save algorithm file", outputMsg);
      }

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

      StringBuffer sbLogs = new StringBuffer(submarineCmd);
      submarineUI.outputLog("Submarine submit command", sbLogs.toString());

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
      AtomicBoolean cmdLineRunning = new AtomicBoolean(true);
      executor.execute(cmdLine, env, new DefaultExecuteResultHandler() {
        @Override
        public void onProcessComplete(int exitValue) {
          String message = String.format(
              "jobName %s ProcessComplete exit value is : %d", jobName, exitValue);
          LOGGER.info(message);
          submarineUI.outputLog("JOR RUN COMPLETE", message);
          cmdLineRunning.set(false);
          submarineJob.setCurrentJobState(EXECUTE_SUBMARINE_FINISHED);
        }
        @Override
        public void onProcessFailed(ExecuteException e) {
          String message = String.format(
              "jobName %s ProcessFailed exit value is : %d, exception is : %s",
              jobName, e.getExitValue(), e.getMessage());
          LOGGER.error(message);
          submarineUI.outputLog("JOR RUN FAILED", message);
          cmdLineRunning.set(false);
          submarineJob.setCurrentJobState(EXECUTE_SUBMARINE_ERROR);
        }
      });
      int loopCount = 100;
      while ((loopCount-- > 0) && cmdLineRunning.get()) {
        Thread.sleep(1000);
      }
      if (watchDog.isWatching()) {
        watchDog.destroyProcess();
        Thread.sleep(1000);
      }
      if (watchDog.isWatching()) {
        watchDog.killedProcess();
      }

      // Check if it has been submitted to YARN
      Map<String, Object> jobState = submarineJob.getJobStateByYarn(jobName);
      loopCount = 50;
      while ((loopCount-- > 0) && !jobState.containsKey("state")) {
        Thread.sleep(3000);
        jobState = submarineJob.getJobStateByYarn(jobName);
      }

      if (!jobState.containsKey("state")) {
        String message = String.format("JOB %s was not submitted to YARN!", jobName);
        LOGGER.error(message);
        submarineUI.outputLog("JOR RUN FAILED", message);
        submarineJob.setCurrentJobState(EXECUTE_SUBMARINE_ERROR);
      }
    } catch (Exception e) {
      LOGGER.error(e.getMessage(), e);
      submarineJob.setCurrentJobState(EXECUTE_SUBMARINE_ERROR);
      submarineUI.outputLog("Exception", e.getMessage());
    } finally {
      running.set(false);
    }
  }
}
