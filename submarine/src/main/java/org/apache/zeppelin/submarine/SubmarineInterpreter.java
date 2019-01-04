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
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.hubspot.jinjava.Jinjava;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.ExecuteException;
import org.apache.commons.exec.ExecuteWatchdog;
import org.apache.commons.exec.LogOutputStream;
import org.apache.commons.exec.PumpStreamHandler;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.zeppelin.conf.ZeppelinConfiguration;
import org.apache.zeppelin.display.AngularObject;
import org.apache.zeppelin.display.ui.OptionInput.ParamOption;
import org.apache.zeppelin.interpreter.Interpreter;
import org.apache.zeppelin.interpreter.InterpreterContext;
import org.apache.zeppelin.interpreter.InterpreterOutput;
import org.apache.zeppelin.interpreter.InterpreterResult;
import org.apache.zeppelin.interpreter.InterpreterException;
import org.apache.zeppelin.interpreter.InterpreterResultMessageOutput;
import org.apache.zeppelin.interpreter.thrift.InterpreterCompletion;
import org.apache.zeppelin.scheduler.Scheduler;
import org.apache.zeppelin.scheduler.SchedulerFactory;
import org.apache.zeppelin.submarine.utils.SubmarineConstants;
import org.apache.zeppelin.submarine.utils.YarnClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * SubmarineInterpreter of Hadoop Submarine implementation.
 * Support for Hadoop Submarine cli. All the commands documented here
 * https://github.com/apache/hadoop/blob/trunk/hadoop-yarn-project/hadoop-yarn/
 * hadoop-yarn-applications/hadoop-yarn-submarine/src/site/markdown/QuickStart.md is supported.
 */
public class SubmarineInterpreter extends Interpreter {
  private Logger LOGGER = LoggerFactory.getLogger(SubmarineInterpreter.class);

  private ZeppelinConfiguration zconf;
  private File pythonWorkDir = null;

  private static final String SUBMARINE_JOBRUN_TF_JINJA
      = "jinja_templates/submarine-job-run-tf.jinja";
  private static final String SUBMARINE_COMMAND_JINJA
      = "jinja_templates/submarine-command.jinja";

  private static final String SUBMARINE_DASHBOARD_JINJA
      = "ui_templates/submarine-dashboard.jinja";
  //private static final String SUBMARINE_COMMAND_RUN_JINJA
  //    = "ui_templates/submarine-command-run.jinja";
  private static final String SUBMARINE_USAGE_JINJA
      = "ui_templates/submarine-usage.jinja";
  private static final String SUBMARINE_COMMAND_OPTIONS_JSON
      = "ui_templates/submarine-command-options.json";
  private static final String SUBMARINE_LOG_HEAD_JINJA
      = "ui_templates/submarine-log-head.jinja";

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

  private boolean needUpdateConfig = true;
  private String currentReplName = "";

  private YarnClient yarnClient = null;

  // Submarine WEB UI
  private Map<String, String> mapMenuUI = new HashMap<>();

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

    yarnClient = new YarnClient(properties);
  }

  @Override
  public void open() {
    LOGGER.info("Command timeout property: {}", getProperty(TIMEOUT_PROPERTY));
  }

  @Override
  public void close() {
  }

  private String createGUI(InterpreterContext context) {
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

  // context.out::List<InterpreterResultMessageOutput> resultMessageOutputs
  // resultMessageOutputs[0] is UI
  // resultMessageOutputs[1] is Execution information
  // resultMessageOutputs[2] is Execution LOG
  private void createResultMessage(InterpreterContext context) {
    if (3 == context.out.size()) {
      return;
    }

    try {
      // The first one is the UI interface.
      context.out.setType(InterpreterResult.Type.ANGULAR);
      // The second is Execution information.
      context.out.setType(InterpreterResult.Type.ANGULAR);
      // The third is log output.
      context.out.setType(InterpreterResult.Type.TEXT);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Override
  public InterpreterResult interpret(String script, InterpreterContext context)
      throws InterpreterException {
    setParagraphConfig(context);
    createResultMessage(context);

    String command = "", active = "";
    String inputPath = "", chkPntPath = "", psLaunchCmd = "", workerLaunchCmd = "";

    LOGGER.debug("Run shell command '" + script + "'");
    OutputStream outStream = new ByteArrayOutputStream();
    String jobName = getJobName(context);
    String noteId = context.getNoteId();

    if (context.getParagraphText().equals(SubmarineConstants.COMMAND_CLEAN)) {
      // Clean Registry Angular Object
      context.getAngularObjectRegistry().removeAll(context.getNoteId(), context.getParagraphId());
    } else {
      command = getAngularObjectValue(context, SubmarineConstants.COMMAND_TYPE);
    }
    active = getAngularObjectValue(context, SubmarineConstants.COMMAND_ACTIVE);
    // clean active
    removeAngularObjectValue(context, SubmarineConstants.COMMAND_ACTIVE);

    inputPath = getAngularObjectValue(context, SubmarineConstants.INPUT_PATH);
    chkPntPath = getAngularObjectValue(context, SubmarineConstants.CHECKPOINT_PATH);
    psLaunchCmd = getAngularObjectValue(context, SubmarineConstants.PS_LAUNCH_CMD);
    workerLaunchCmd = getAngularObjectValue(context, SubmarineConstants.WORKER_LAUNCH_CMD);

    try {
      /*
      CommandParser commandParser = new CommandParser();
      commandParser.populate(script);

      // Get the set variables from the user's execution script
      String inputPath = commandParser.getConfig(SubmarineConstants.INPUT_PATH, "");
      String chkPntPath = commandParser.getConfig(SubmarineConstants.CHECKPOINT_PATH, "");
      String psLaunchCmd = commandParser.getConfig(SubmarineConstants.PS_LAUNCH_CMD, "");
      String workerLaunchCmd = commandParser.getConfig(SubmarineConstants.WORKER_LAUNCH_CMD, "");*/

      properties.put(SubmarineConstants.INPUT_PATH, inputPath != null ? inputPath : "");
      properties.put(SubmarineConstants.CHECKPOINT_PATH, chkPntPath != null ? chkPntPath : "");
      properties.put(SubmarineConstants.PS_LAUNCH_CMD, psLaunchCmd != null ? psLaunchCmd : "");
      properties.put(SubmarineConstants.WORKER_LAUNCH_CMD,
          workerLaunchCmd != null ? workerLaunchCmd : "");

      // String command = commandParser.getCommand();
      CommandLine cmdLine = CommandLine.parse(shell);
      cmdLine.addArgument(script, false);

      SubmarineCommand submarineCmd = SubmarineCommand.fromCommand(command);
      createSubmarineUI(submarineCmd, context);

      switch (submarineCmd) {
        case USAGE:
          /*
          Map<String, String> infos = new java.util.HashMap<>();
          infos.put("jobUrl", "Tensorboard|http://192.168.0.1/tensorboard");
          infos.put("label", "Submarine UI");
          infos.put("tooltip", "View in Submarine web UI");
          infos.put("noteId", context.getNoteId());
          infos.put("paraId", context.getParagraphId());
          context.getIntpEventClient().onParaInfosReceived(infos);

          Map<String, String> infos2 = new java.util.HashMap<>();
          infos2.put("jobUrl", "Parameter server|http://192.168.0.1/ps");
          infos2.put("label", "Submarine UI");
          infos2.put("tooltip", "View in Submarine web UI");
          infos2.put("noteId", context.getNoteId());
          infos2.put("paraId", context.getParagraphId());
          context.getIntpEventClient().onParaInfosReceived(infos2);

          Map<String, String> infos3 = new java.util.HashMap<>();
          infos3.put("jobUrl", "Worker server|http://192.168.0.1/ws");
          infos3.put("label", "Submarine UI");
          infos3.put("tooltip", "View in Submarine web UI");
          infos3.put("noteId", context.getNoteId());
          infos3.put("paraId", context.getParagraphId());
          context.getIntpEventClient().onParaInfosReceived(infos3);
          */

          // String message = getSubmarineHelp();
          //printUsageUI(context);
          return new InterpreterResult(InterpreterResult.Code.SUCCESS);
        case JOB_RUN:
          if (!active.isEmpty()) {
            createLogHeadUI(context);
            return jobRun(jobName, noteId, context);
          }
          break;
        case JOB_SHOW:
          if (!active.isEmpty()) {
            createLogHeadUI(context);
            return jobShow(submarineCmd, jobName, noteId, context);
          }
          break;
        case JOB_LIST:
          if (!active.isEmpty()) {
            createLogHeadUI(context);
            return jobRun(jobName, noteId, context);
          }
          break;
        case OLD_UI:
          createGUI(context);
          break;
        case EMPTY:
          return new InterpreterResult(InterpreterResult.Code.SUCCESS);
        default:
          createLogHeadUI(context);
          String message = "ERROR: Unknown command [" + command + "]";
          outputLog(context, "Unknown command", message, true);
          return new InterpreterResult(InterpreterResult.Code.ERROR);
      }
    } catch (ExecuteException e) {
      int exitValue = e.getExitValue();
      LOGGER.error("Can not run " + script, e);
      InterpreterResult.Code code = InterpreterResult.Code.ERROR;
      String message = outStream.toString();
      if (exitValue == 143) {
        code = InterpreterResult.Code.INCOMPLETE;
        message += "Paragraph received a SIGTERM\n";
        LOGGER.info("The paragraph " + context.getParagraphId()
            + " stopped executing: " + message);
      }
      message += "ExitValue: " + exitValue;
      outputLog(context, "Execute exception", message, true);
      return new InterpreterResult(code, message);
    } catch (Exception e) {
      if (Boolean.parseBoolean(getProperty("zeppelin.submarine.stacktrace"))) {
        throw new InterpreterException(e);
      }
      LOGGER.error("Can not run " + script, e);
      String message = outStream.toString() + "\n" + e.getMessage();
      outputLog(context, "Execute exception", message, true);
      return new InterpreterResult(InterpreterResult.Code.ERROR);
    } finally {

    }

    return new InterpreterResult(InterpreterResult.Code.SUCCESS);
  }

  private String getAngularObjectValue(InterpreterContext context, String name) {
    String value = "";
    AngularObject angularObject = context.getAngularObjectRegistry()
        .get(name, context.getNoteId(), context.getParagraphId());
    if (null != angularObject && null != angularObject.get()) {
      value = angularObject.get().toString();
    }
    return value;
  }

  private void removeAngularObjectValue(InterpreterContext context, String name) {
    context.getAngularObjectRegistry().remove(name, context.getNoteId(), context.getParagraphId());
  }

  private void createSubmarineUI(SubmarineCommand submarineCmd, InterpreterContext context) {
    try {
      HashMap<String, Object> mapParams = new HashMap();
      mapParams.put(unifyKey(SubmarineConstants.PARAGRAPH_ID), context.getParagraphId());
      mapParams.put(unifyKey(SubmarineConstants.COMMAND_TYPE), submarineCmd.getCommand());

      String templateName = "";
      switch (submarineCmd) {
        case USAGE:
          templateName = SUBMARINE_USAGE_JINJA;
          List<CommandlineOption> commandlineOptions = getCommandlineOptions();
          mapParams.put(SubmarineConstants.COMMANDLINE_OPTIONS, commandlineOptions);
          break;
        default:
          templateName = SUBMARINE_DASHBOARD_JINJA;
          break;
      }

      URL urlTemplate = Resources.getResource(templateName);
      String template = Resources.toString(urlTemplate, Charsets.UTF_8);
      Jinjava jinjava = new Jinjava();
      String submarineUsage = jinjava.render(template, mapParams);

      // UI
      InterpreterResultMessageOutput outputUI = context.out.getOutputAt(0);
      outputUI.clear();
      outputUI.write(submarineUsage);
      outputUI.flush();

      // UI update, log needs to be cleaned at the same time
      InterpreterResultMessageOutput outputLOG = context.out.getOutputAt(1);
      outputLOG.clear();
      outputLOG.flush();
    } catch (IOException e) {
      LOGGER.error("Can't print usage", e);
    }
  }

  private void printUsageUI(InterpreterContext context) {
    try {
      List<CommandlineOption> commandlineOptions = getCommandlineOptions();
      HashMap<String, Object> mapParams = new HashMap();
      mapParams.put(unifyKey(SubmarineConstants.PARAGRAPH_ID), context.getParagraphId());
      mapParams.put(SubmarineConstants.COMMANDLINE_OPTIONS, commandlineOptions);

      URL urlTemplate = Resources.getResource(SUBMARINE_USAGE_JINJA);
      String template = Resources.toString(urlTemplate, Charsets.UTF_8);
      Jinjava jinjava = new Jinjava();
      String submarineUsage = jinjava.render(template, mapParams);

      InterpreterResultMessageOutput outputUI = context.out.getOutputAt(0);
      outputUI.clear();
      outputUI.write(submarineUsage);
      outputUI.flush();

      // UI update, log needs to be cleaned at the same time
      InterpreterResultMessageOutput outputLOG = context.out.getOutputAt(1);
      outputLOG.clear();
      outputLOG.flush();
    } catch (IOException e) {
      LOGGER.error("Can't print usage", e);
    }
  }

  public List<CommandlineOption> getCommandlineOptions() throws IOException {
    List<CommandlineOption> commandlineOptions = new ArrayList<>();

    BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(
        this.getClass().getResourceAsStream("/" + SUBMARINE_COMMAND_OPTIONS_JSON)));

    String line;
    StringBuffer strbuf = new StringBuffer();
    int licensedLineCount = 14;
    while ((line = bufferedReader.readLine()) != null) {
      if (licensedLineCount-- > 0) {
        continue;
      }
      strbuf.append(line);
    }
    Gson gson = new Gson();
    commandlineOptions = gson.fromJson(strbuf.toString(),
        new TypeToken<List<CommandlineOption>>() {
        }.getType());

    return commandlineOptions;
  }

  private void createLogHeadUI(InterpreterContext context) {
    try {
      HashMap<String, Object> mapParams = new HashMap();
      URL urlTemplate = Resources.getResource(SUBMARINE_LOG_HEAD_JINJA);
      String template = Resources.toString(urlTemplate, Charsets.UTF_8);
      Jinjava jinjava = new Jinjava();
      String submarineUsage = jinjava.render(template, mapParams);

      InterpreterResultMessageOutput outputUI = context.out.getOutputAt(1);
      outputUI.clear();
      outputUI.write(submarineUsage);
      outputUI.flush();
    } catch (IOException e) {
      LOGGER.error("Can't print usage", e);
    }
  }

  private void printCommnadUI(SubmarineCommand submarineCmd, InterpreterContext context) {
    try {
      HashMap<String, Object> mapParams = new HashMap();
      mapParams.put(unifyKey(SubmarineConstants.PARAGRAPH_ID), context.getParagraphId());

      URL urlTemplate = Resources.getResource(SUBMARINE_DASHBOARD_JINJA);
      String template = Resources.toString(urlTemplate, Charsets.UTF_8);
      Jinjava jinjava = new Jinjava();
      String submarineUsage = jinjava.render(template, mapParams);

      InterpreterResultMessageOutput outputUI = context.out.getOutputAt(0);
      outputUI.clear();
      outputUI.write(submarineUsage);
      outputUI.flush();

      // UI update, log needs to be cleaned at the same time
      InterpreterResultMessageOutput outputLOG = context.out.getOutputAt(1);
      outputLOG.clear();
      outputLOG.flush();
    } catch (IOException e) {
      LOGGER.error("Can't print usage", e);
    }
  }

  private void outputLog(InterpreterContext context, String title,
                         String message, boolean format) {
    try {
      StringBuffer formatMsg = new StringBuffer();
      InterpreterResultMessageOutput output = null;
      if (format) {
        formatMsg.append("<div style=\"width:100%\">");
        formatMsg.append(title);
        formatMsg.append("<pre>");
        formatMsg.append(message);
        formatMsg.append("</pre>");
        formatMsg.append("</div>\n");
        output = context.out.getOutputAt(1);
        message = formatMsg.toString();
      } else {
        output = context.out.getOutputAt(2);
        message = message + "\n";
      }
      output.write(message);
      output.flush();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Override
  public void cancel(InterpreterContext context) {
    String jobName = getJobName(context);
    submarineCommand(SubmarineCommand.JOB_CANCEL, jobName, context);
  }

  @Override
  public FormType getFormType() {
    return FormType.SIMPLE;
  }

  @Override
  public int getProgress(InterpreterContext context) {
    String jobName = getJobName(context);
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
        infos.put("jobUrl", "Yarn log|" + sbUrl.toString());
        infos.put("label", "Submarine WEB");
        infos.put("tooltip", "View in Submarine web UI");
        infos.put("noteId", context.getNoteId());
        infos.put("paraId", context.getParagraphId());
        context.getIntpEventClient().onParaInfosReceived(infos);

        Map<String, String> infos2 = new java.util.HashMap<>();
        infos2.put("jobUrl", "Tensorboard|http://192.168.0.1/tensorboard");
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

  public void setPythonWorkDir(File pythonWorkDir) {
    this.pythonWorkDir = pythonWorkDir;
  }

  // yarn application match the pattern [a-z][a-z0-9-]*
  private String getJobName(InterpreterContext contextIntp) {
    return "submarine-" + contextIntp.getNoteId().toLowerCase();
  }

  private InterpreterResult jobRun(String jobName, String noteId,
                                   InterpreterContext context)
      throws IOException {
    String algorithmPath = this.properties.getProperty(
        SubmarineConstants.SUBMARINE_ALGORITHM_HDFS_PATH);
    if (!algorithmPath.startsWith("hdfs://")) {
      String message = "Algorithm file upload HDFS path, " +
          "Must be `hdfs://` prefix. now setting " + algorithmPath;
      outputLog(context, "Configuration error", message, true);
      return new InterpreterResult(InterpreterResult.Code.ERROR);
    }

    /*
    Properties properties = submarineContext.getProperties(noteId);
    if (null == properties) {
      properties = this.properties;
      submarineContext.setProperties(noteId, properties);
      properties.put(SubmarineConstants.JOB_NAME, jobName);
    }*/

    String outputMsg = submarineContext.saveParagraphToFiles(noteId, context.getNoteName(),
        pythonWorkDir == null ? "" : pythonWorkDir.getAbsolutePath(), properties);
    outputLog(context, "Save algorithm file", outputMsg, true);

    HashMap jinjaParams = propertiesToJinjaParams(jobName, context, true);

    URL urlTemplate = Resources.getResource(SUBMARINE_JOBRUN_TF_JINJA);
    String template = Resources.toString(urlTemplate, Charsets.UTF_8);
    Jinjava jinjava = new Jinjava();
    String submarineCmd = jinjava.render(template, jinjaParams);

    LOGGER.info("Execute : " + submarineCmd);

    StringBuffer sbLogs = new StringBuffer();
    sbLogs.append("Submarine submit job : " + jobName);
    sbLogs.append(submarineCmd);
    outputLog(context, "Submarine submit command", sbLogs.toString(), true);

    CommandLine cmdLine = CommandLine.parse(shell);
    cmdLine.addArgument(submarineCmd, false);
    DefaultExecutor executor = new DefaultExecutor();
    executor.setStreamHandler(new PumpStreamHandler(context.out, context.out));
    long timeout = Long.valueOf(getProperty(TIMEOUT_PROPERTY, defaultTimeoutProperty));

    executor.setWatchdog(new ExecuteWatchdog(timeout));
    if (Boolean.valueOf(getProperty(DIRECTORY_USER_HOME))) {
      executor.setWorkingDirectory(new File(System.getProperty("user.home")));
    }

    int exitVal = executor.execute(cmdLine);
    LOGGER.info("jobName {} return with exit value: {}", jobName, exitVal);

    return new InterpreterResult(InterpreterResult.Code.SUCCESS, context.out.toString());
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
  private HashMap propertiesToJinjaParams(String jobName, InterpreterContext context,
                                          boolean outputLog)
      throws IOException {
    StringBuffer sbMessage = new StringBuffer();

    // Check user-set job variables
    String inputPath = properties.getProperty(SubmarineConstants.INPUT_PATH);
    if (StringUtils.isEmpty(inputPath) && outputLog) {
      setUserPropertiesWarn(sbMessage, SubmarineConstants.INPUT_PATH, "=path...\n");
    }
    String checkPointPath = properties.getProperty(SubmarineConstants.CHECKPOINT_PATH);
    if (StringUtils.isEmpty(checkPointPath) && outputLog) {
      setUserPropertiesWarn(sbMessage, SubmarineConstants.CHECKPOINT_PATH, "=path...\n");
    }
    String psLaunchCmd = properties.getProperty(SubmarineConstants.PS_LAUNCH_CMD);
    if (StringUtils.isEmpty(psLaunchCmd) && outputLog) {
      setUserPropertiesWarn(sbMessage, SubmarineConstants.PS_LAUNCH_CMD,
          "=python cifar10_main.py " +
              "--data-dir=hdfs://mldev/tmp/cifar-10-data " +
              "--job-dir=hdfs://mldev/tmp/cifar-10-jobdir --num-gpus=0\n");
    }
    String workerLaunchCmd = properties.getProperty(SubmarineConstants.WORKER_LAUNCH_CMD);
    if (StringUtils.isEmpty(workerLaunchCmd) && outputLog) {
      setUserPropertiesWarn(sbMessage, SubmarineConstants.WORKER_LAUNCH_CMD,
          "=python cifar10_main.py " +
              "--data-dir=hdfs://mldev/tmp/cifar-10-data " +
              "--job-dir=hdfs://mldev/tmp/cifar-10-jobdir " +
              "--train-steps=500 --eval-batch-size=16 --train-batch-size=16 --sync --num-gpus=1\n");
    }

    // Check interpretere set Properties
    if (StringUtils.isEmpty(submarineHadoopHome) && outputLog) {
      setIntpPropertiesWarn(sbMessage, SubmarineConstants.SUBMARINE_HADOOP_HOME);
    }
    File file = new File(submarineHadoopHome);
    if (!file.exists()) {
      sbMessage.append(SubmarineConstants.HADOOP_YARN_SUBMARINE_JAR + ": "
          + submarineHadoopHome + " is not a valid file path!\n");
    }
    if (StringUtils.isEmpty(submarineJar) && outputLog) {
      setIntpPropertiesWarn(sbMessage, SubmarineConstants.HADOOP_YARN_SUBMARINE_JAR);
    }
    file = new File(submarineJar);
    if (!file.exists()) {
      sbMessage.append(SubmarineConstants.HADOOP_YARN_SUBMARINE_JAR + ":"
          + submarineJar + " is not a valid file path!\n");
    }
    String containerNetwork = properties.getProperty(
        SubmarineConstants.DOCKER_CONTAINER_NETWORK, "");
    if (StringUtils.isEmpty(containerNetwork) && outputLog) {
      setIntpPropertiesWarn(sbMessage, SubmarineConstants.DOCKER_CONTAINER_NETWORK);
    }
    String parameterServicesImage = properties.getProperty(
        SubmarineConstants.TF_PARAMETER_SERVICES_DOCKER_IMAGE, "");
    if (StringUtils.isEmpty(parameterServicesImage) && outputLog) {
      setIntpPropertiesWarn(sbMessage, SubmarineConstants.TF_PARAMETER_SERVICES_DOCKER_IMAGE);
    }
    String parameterServicesNum = properties.getProperty(
        SubmarineConstants.TF_PARAMETER_SERVICES_NUM, "");
    if (StringUtils.isEmpty(parameterServicesNum) && outputLog) {
      setIntpPropertiesWarn(sbMessage, SubmarineConstants.TF_PARAMETER_SERVICES_NUM);
    }
    String parameterServicesGpu = properties.getProperty(
        SubmarineConstants.TF_PARAMETER_SERVICES_GPU, "");
    if (StringUtils.isEmpty(parameterServicesGpu) && outputLog) {
      setIntpPropertiesWarn(sbMessage, SubmarineConstants.TF_PARAMETER_SERVICES_GPU);
    }
    String parameterServicesCpu = properties.getProperty(
        SubmarineConstants.TF_PARAMETER_SERVICES_CPU, "");
    if (StringUtils.isEmpty(parameterServicesCpu) && outputLog) {
      setIntpPropertiesWarn(sbMessage, SubmarineConstants.TF_PARAMETER_SERVICES_CPU);
    }
    String parameterServicesMemory = properties.getProperty(
        SubmarineConstants.TF_PARAMETER_SERVICES_MEMORY, "");
    if (StringUtils.isEmpty(parameterServicesMemory) && outputLog) {
      setIntpPropertiesWarn(sbMessage, SubmarineConstants.TF_PARAMETER_SERVICES_MEMORY);
    }
    String workerServicesImage = properties.getProperty(
        SubmarineConstants.TF_WORKER_SERVICES_DOCKER_IMAGE, "");
    if (StringUtils.isEmpty(workerServicesImage) && outputLog) {
      setIntpPropertiesWarn(sbMessage, SubmarineConstants.TF_WORKER_SERVICES_DOCKER_IMAGE);
    }
    String workerServicesNum = properties.getProperty(
        SubmarineConstants.TF_WORKER_SERVICES_NUM, "");
    if (StringUtils.isEmpty(workerServicesNum) && outputLog) {
      setIntpPropertiesWarn(sbMessage, SubmarineConstants.TF_WORKER_SERVICES_NUM);
    }
    String workerServicesGpu = properties.getProperty(
        SubmarineConstants.TF_WORKER_SERVICES_GPU, "");
    if (StringUtils.isEmpty(workerServicesGpu) && outputLog) {
      setIntpPropertiesWarn(sbMessage, SubmarineConstants.TF_WORKER_SERVICES_GPU);
    }
    String workerServicesCpu = properties.getProperty(
        SubmarineConstants.TF_WORKER_SERVICES_CPU, "");
    if (StringUtils.isEmpty(workerServicesCpu) && outputLog) {
      setIntpPropertiesWarn(sbMessage, SubmarineConstants.TF_WORKER_SERVICES_CPU);
    }
    String workerServicesMemory = properties.getProperty(
        SubmarineConstants.TF_WORKER_SERVICES_MEMORY, "");
    if (StringUtils.isEmpty(workerServicesMemory) && outputLog) {
      setIntpPropertiesWarn(sbMessage, SubmarineConstants.TF_WORKER_SERVICES_MEMORY);
    }
    String algorithmUploadPath = getProperty(
        SubmarineConstants.SUBMARINE_ALGORITHM_HDFS_PATH, "");
    if (StringUtils.isEmpty(algorithmUploadPath) && outputLog) {
      setIntpPropertiesWarn(sbMessage, SubmarineConstants.SUBMARINE_ALGORITHM_HDFS_PATH);
    }
    String submarineHadoopKeytab = getProperty(
        SubmarineConstants.SUBMARINE_HADOOP_KEYTAB, "");
    if (StringUtils.isEmpty(submarineHadoopKeytab) && outputLog) {
      setIntpPropertiesWarn(sbMessage, SubmarineConstants.SUBMARINE_HADOOP_KEYTAB);
    }
    file = new File(submarineHadoopKeytab);
    if (!file.exists()) {
      sbMessage.append(SubmarineConstants.SUBMARINE_HADOOP_KEYTAB + ":"
          + submarineHadoopKeytab + " is not a valid file path!\n");
    }
    String submarineHadoopPrincipal = getProperty(
        SubmarineConstants.SUBMARINE_HADOOP_PRINCIPAL, "");
    if (StringUtils.isEmpty(submarineHadoopKeytab) && outputLog) {
      setIntpPropertiesWarn(sbMessage, SubmarineConstants.SUBMARINE_HADOOP_PRINCIPAL);
    }
    String machinelearingDistributed = getProperty(
        SubmarineConstants.MACHINELEARING_DISTRIBUTED_ENABLE, "false");

    String notePath = algorithmUploadPath + File.separator + context.getNoteId();
    List<String> arrayHdfsFiles = new ArrayList<>();
    List<Path> hdfsFiles = submarineContext.getHDFSUtils().list(new Path(notePath + "/*"));
    if (hdfsFiles.size() == 0) {
      sbMessage.append("ERROR: The " + notePath + " file directory was is empty in HDFS!\n");
    } else {
      if (outputLog) {
        StringBuffer sbCommitFiles = new StringBuffer();
        sbCommitFiles.append("INFO: You commit total of " + hdfsFiles.size()
            + " algorithm files.\n");
        for (int i = 0; i < hdfsFiles.size(); i++) {
          String filePath = hdfsFiles.get(i).toUri().toString();
          arrayHdfsFiles.add(filePath);
          sbCommitFiles.append("INFO: [" + hdfsFiles.get(i).getName() + "] -> " + filePath + "\n");
        }
        outputLog(context, "Execution information", sbCommitFiles.toString(), true);
      }
    }

    // Found null variable, throw exception
    if (!StringUtils.isEmpty(sbMessage.toString()) && outputLog) {
      throw new RuntimeException(sbMessage.toString());
    }

    // Save user-set variables and interpreter configuration parameters
    HashMap<String, Object> mapParams = new HashMap();
    mapParams.put(unifyKey(SubmarineConstants.SUBMARINE_HADOOP_HOME), submarineHadoopHome);
    mapParams.put(unifyKey(SubmarineConstants.HADOOP_YARN_SUBMARINE_JAR), submarineJar);
    mapParams.put(unifyKey(SubmarineConstants.JOB_NAME), jobName);
    mapParams.put(unifyKey(SubmarineConstants.DOCKER_CONTAINER_NETWORK), containerNetwork);
    mapParams.put(unifyKey(SubmarineConstants.SUBMARINE_HADOOP_KEYTAB), submarineHadoopKeytab);
    mapParams.put(unifyKey(SubmarineConstants.SUBMARINE_HADOOP_PRINCIPAL),
        submarineHadoopPrincipal);
    if (machinelearingDistributed.equals("true")) {
      mapParams.put(unifyKey(SubmarineConstants.MACHINELEARING_DISTRIBUTED_ENABLE),
          machinelearingDistributed);
    }
    mapParams.put(unifyKey(SubmarineConstants.SUBMARINE_ALGORITHM_HDFS_PATH), notePath);
    mapParams.put(unifyKey(SubmarineConstants.SUBMARINE_ALGORITHM_HDFS_FILES), arrayHdfsFiles);
    mapParams.put(unifyKey(SubmarineConstants.INPUT_PATH), inputPath);
    mapParams.put(unifyKey(SubmarineConstants.CHECKPOINT_PATH), checkPointPath);
    mapParams.put(unifyKey(SubmarineConstants.PS_LAUNCH_CMD), psLaunchCmd);
    mapParams.put(unifyKey(SubmarineConstants.WORKER_LAUNCH_CMD), workerLaunchCmd);
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

  private InterpreterResult jobShow(SubmarineCommand command, String jobName, String noteId,
                                    InterpreterContext context) {
    Map<String, Object> mapStatus = yarnClient.getAppStatus(jobName);

    return new InterpreterResult(InterpreterResult.Code.SUCCESS);
  }

  private InterpreterResult submarineCommand(
      SubmarineCommand command, String jobName, InterpreterContext context) {
    HashMap jinjaParams = null;
    try {
      jinjaParams = propertiesToJinjaParams(jobName, context, false);
      jinjaParams.put(unifyKey(SubmarineConstants.JOB_NAME), jobName);
      jinjaParams.put(unifyKey(SubmarineConstants.COMMAND_TYPE), command.getCommand());

      URL urlTemplate = Resources.getResource(SUBMARINE_COMMAND_JINJA);
      String template = Resources.toString(urlTemplate, Charsets.UTF_8);
      Jinjava jinjava = new Jinjava();
      String submarineCmd = jinjava.render(template, jinjaParams);

      LOGGER.info("Execute : " + submarineCmd);
      outputLog(context, "Submarine submit command", submarineCmd, true);
      CommandLine cmdLine = CommandLine.parse(shell);
      cmdLine.addArgument(submarineCmd, false);

      InterpreterOutput interpreterOutput = context.out;

      DefaultExecutor executor = new DefaultExecutor();

      StringBuffer sbLogOutput = new StringBuffer();
      executor.setStreamHandler(new PumpStreamHandler(new LogOutputStream() {
        @Override
        protected void processLine(String line, int level) {
          sbLogOutput.append(line);
          outputLog(context, "", line, false);
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
    } catch (IOException e) {
      e.printStackTrace();
    }

    return new InterpreterResult(InterpreterResult.Code.SUCCESS);
  }

  private String unifyKey(String key) {
    key = key.replace(".", "_").toUpperCase();
    return key;
  }

  /**
   * submarine Commandline Option
   */
  class CommandlineOption {
    private String name;
    private String description;

    CommandlineOption(String name, String description) {
      this.name = name;
      this.description = description;
    }

    public String getName() {
      return name;
    }

    public String getDescription() {
      return description;
    }
  }

  public enum SubmarineCommand {
    USAGE("USAGE"),
    JOB_RUN("JOB RUN"),
    JOB_SHOW("JOB SHOW"),
    JOB_LIST("JOB LIST"),
    JOB_CANCEL("JOB CANCEL"),
    EMPTY(""),
    OLD_UI("OLD UI"),
    UNKNOWN("Unknown");

    private String command;

    SubmarineCommand(String command){
      this.command = command;
    }

    public String getCommand(){
      return command;
    }

    public static SubmarineCommand fromCommand(String command)
    {
      for (SubmarineCommand type : SubmarineCommand.values()) {
        if (type.getCommand().equals(command)) {
          return type;
        }
      }

      return UNKNOWN;
    }
  }
}
