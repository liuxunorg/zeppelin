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
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.yarn.submarine.client.cli.CliConstants;
import org.apache.zeppelin.interpreter.KerberosInterpreter;
import org.apache.zeppelin.interpreter.InterpreterContext;
import org.apache.zeppelin.interpreter.InterpreterResult;
import org.apache.zeppelin.interpreter.InterpreterException;
import org.apache.zeppelin.interpreter.InterpreterOutput;
import org.apache.zeppelin.interpreter.thrift.InterpreterCompletion;
import org.apache.zeppelin.scheduler.Scheduler;
import org.apache.zeppelin.scheduler.SchedulerFactory;
import org.apache.zeppelin.submarine.utils.SubmarineParagraph;
import org.apache.zeppelin.submarine.utils.SubmarineUtils;
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
import java.util.concurrent.ConcurrentHashMap;

/**
 * SubmarineInterpreter of Hadoop Submarine implementation.
 * Support for Hadoop Submarine cli. All the commands documented here
 * https://github.com/apache/hadoop/blob/trunk/hadoop-yarn-project/hadoop-yarn/
 * hadoop-yarn-applications/hadoop-yarn-submarine/src/site/markdown/QuickStart.md is supported.
 */
public class SubmarineInterpreter extends KerberosInterpreter {
  private Logger LOGGER = LoggerFactory.getLogger(SubmarineInterpreter.class);

  public static final String HADOOP_HOME = "HADOOP_HOME";
  public static final String HADOOP_CONF_DIR = "HADOOP_CONF_DIR";
  public static final String HADOOP_YARN_SUBMARINE_JAR = "hadoop.yarn.submarine.jar";
  public static final String SUBMARINE_YARN_QUEUE      = "submarine.yarn.queue";
  public static final String DOCKER_CONTAINER_NETWORK  = "docker.container.network";
  public static final String SUBMARINE_CONCURRENT_MAX = "submarine.concurrent.max";

  public static final String SUBMARINE_HDFS_KEYTAB    = "submarine.hdfs.keytab";
  public static final String SUBMARINE_HDFS_PRINCIPAL = "submarine.hdfs.principal";

  public static final String ALGORITHM_UPLOAD_PATH    = "algorithm.upload.path";

  // Number of submarines executed in parallel for each interpreter instance
  protected int concurrentExecutedMax = 1;

  private static final String DIRECTORY_USER_HOME = "shell.working.directory.user.home";
  private final boolean isWindows = System.getProperty("os.name").startsWith("Windows");
  private final String shell = isWindows ? "cmd /c" : "bash -c";

  private static final String TIMEOUT_PROPERTY = "submarine.command.timeout.millisecond";
  private String defaultTimeoutProperty = "60000";
  ConcurrentHashMap<String, DefaultExecutor> executors;

  private String submarineJar = "";
  private String hadoopHome = "";

  public SubmarineInterpreter(Properties property) {
    super(property);

    concurrentExecutedMax = Integer.parseInt(getProperty(SUBMARINE_CONCURRENT_MAX, "1"));
    hadoopHome = property.getProperty(SubmarineInterpreter.HADOOP_HOME, "");
    submarineJar = property.getProperty(SubmarineInterpreter.HADOOP_YARN_SUBMARINE_JAR, "");
  }

  @Override
  public void open() {
    super.open();
    LOGGER.info("Command timeout property: {}", getProperty(TIMEOUT_PROPERTY));
    executors = new ConcurrentHashMap<>();
  }

  @Override
  public void close() {
    super.close();
    for (String executorKey : executors.keySet()) {
      DefaultExecutor executor = executors.remove(executorKey);
      if (executor != null) {
        try {
          executor.getWatchdog().destroyProcess();
        } catch (Exception e){
          LOGGER.error("error destroying executor for paragraphId: " + executorKey, e);
        }
      }
    }
  }

  @Override
  public InterpreterResult interpret(String originalCmd, InterpreterContext contextInterpreter) {
    String cmd = Boolean.parseBoolean(getProperty("zeppelin.shell.interpolation")) ?
        interpolate(originalCmd, contextInterpreter.getResourcePool()) : originalCmd;
    LOGGER.debug("Run shell command '" + cmd + "'");

    SubmarineParagraph submarineParagraph = new SubmarineParagraph(
        contextInterpreter.getNoteId(),
        contextInterpreter.getNoteName(),
        contextInterpreter.getParagraphId(),
        contextInterpreter.getParagraphTitle(),
        contextInterpreter.getParagraphText(),
        contextInterpreter.getReplName(), cmd);

    OutputStream outStream = new ByteArrayOutputStream();

    CommandLine cmdLine = CommandLine.parse(shell);
    // the Windows CMD shell doesn't handle multiline statements,
    // they need to be delimited by '&&' instead
    if (isWindows) {
      String[] lines = StringUtils.split(cmd, "\n");
      cmd = StringUtils.join(lines, " && ");
    }
    cmdLine.addArgument(cmd, false);

    try {
      DefaultExecutor executor = null;
      String cmdType = SubmarineUtils.parseCommand(cmd);
      if (cmdType.equalsIgnoreCase("help")) {
        String message = getSubmarineHelp();
        return new InterpreterResult(InterpreterResult.Code.SUCCESS, message);
      } else if (cmdType.equals(CliConstants.SHOW)) {
        executor = showJob(submarineParagraph, contextInterpreter.out);
      } else if (cmdType.equals(CliConstants.RUN)) {

      } else {
        String message = "Unsupported command!\n";
        message += getSubmarineHelp();
        return new InterpreterResult(InterpreterResult.Code.ERROR, message);
      }
      executors.put(contextInterpreter.getParagraphId(), executor);

      int exitVal = executor.execute(cmdLine);
      LOGGER.info("Paragraph " + contextInterpreter.getParagraphId()
          + " return with exit value: " + exitVal);
      return new InterpreterResult(InterpreterResult.Code.SUCCESS, outStream.toString());
    } catch (ExecuteException e) {
      int exitValue = e.getExitValue();
      LOGGER.error("Can not run " + cmd, e);
      InterpreterResult.Code code = InterpreterResult.Code.ERROR;
      String message = outStream.toString();
      if (exitValue == 143) {
        code = InterpreterResult.Code.INCOMPLETE;
        message += "Paragraph received a SIGTERM\n";
        LOGGER.info("The paragraph " + contextInterpreter.getParagraphId()
            + " stopped executing: " + message);
      }
      message += "ExitValue: " + exitValue;
      return new InterpreterResult(code, message);
    } catch (IOException e) {
      LOGGER.error("Can not run " + cmd, e);
      return new InterpreterResult(InterpreterResult.Code.ERROR, e.getMessage());
    } finally {
      executors.remove(contextInterpreter.getParagraphId());
    }
  }

  @Override
  public void cancel(InterpreterContext context) {
    DefaultExecutor executor = executors.remove(context.getParagraphId());
    if (executor != null) {
      try {
        executor.getWatchdog().destroyProcess();
      } catch (Exception e){
        LOGGER.error("error destroying executor for paragraphId: " + context.getParagraphId(), e);
      }
    }
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

  public DefaultExecutor runJob(SubmarineParagraph paragraph, InterpreterOutput output) {
    HashMap mapParams = new HashMap();

    URL urlTemplate = Resources.getResource("submarine-job-run-tf.jinja");
    String template = null;
    try {
      template = Resources.toString(urlTemplate, Charsets.UTF_8);
    } catch (IOException e) {
      e.printStackTrace();
    }

    Jinjava jinjava = new Jinjava();
    String renderedTemplate = jinjava.render(template, mapParams);

    try {
      output.write(renderedTemplate);
    } catch (IOException e) {
      e.printStackTrace();
      return null;
    }
    String cmd = "ls "; // command.toString();

    CommandLine cmdLine = CommandLine.parse(shell);
    // the Windows CMD shell doesn't handle multiline statements,
    // they need to be delimited by '&&' instead
    if (isWindows) {
      String[] lines = StringUtils.split(cmd, "\n");
      cmd = StringUtils.join(lines, " && ");
    }
    cmdLine.addArgument(cmd, false);

    DefaultExecutor executor = new DefaultExecutor();
    executor.setStreamHandler(new PumpStreamHandler(output, output));

    long timeout = Long.valueOf(getProperty(TIMEOUT_PROPERTY, defaultTimeoutProperty));

    executor.setWatchdog(new ExecuteWatchdog(timeout));
    if (Boolean.valueOf(getProperty(DIRECTORY_USER_HOME))) {
      executor.setWorkingDirectory(new File(System.getProperty("user.home")));
    }

    return executor;
  }

  public DefaultExecutor showJob(SubmarineParagraph paragraph, InterpreterOutput output) {
    String yarnPath = hadoopHome + "/bin/yarn";

    StringBuffer command = new StringBuffer();
    command.append(yarnPath).append(" ");
    command.append(submarineJar).append(" ");
    command.append("job").append(" ");
    command.append("show").append(" ");
    command.append("--name").append(" ");
    command.append(paragraph.getNoteId() + "-" + paragraph.getParagraphId());

    try {
      output.write(command.toString());
    } catch (IOException e) {
      e.printStackTrace();
      return null;
    }
    String cmd = "ls "; // command.toString();

    CommandLine cmdLine = CommandLine.parse(shell);
    // the Windows CMD shell doesn't handle multiline statements,
    // they need to be delimited by '&&' instead
    if (isWindows) {
      String[] lines = StringUtils.split(cmd, "\n");
      cmd = StringUtils.join(lines, " && ");
    }
    cmdLine.addArgument(cmd, false);

    DefaultExecutor executor = new DefaultExecutor();
    executor.setStreamHandler(new PumpStreamHandler(output, output));

    long timeout = Long.valueOf(getProperty(TIMEOUT_PROPERTY, defaultTimeoutProperty));

    executor.setWatchdog(new ExecuteWatchdog(timeout));
    if (Boolean.valueOf(getProperty(DIRECTORY_USER_HOME))) {
      executor.setWorkingDirectory(new File(System.getProperty("user.home")));
    }

    return executor;
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
