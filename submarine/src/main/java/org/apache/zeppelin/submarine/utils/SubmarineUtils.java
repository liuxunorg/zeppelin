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

package org.apache.zeppelin.submarine.utils;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.zeppelin.display.AngularObject;
import org.apache.zeppelin.interpreter.InterpreterContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;

public class SubmarineUtils {
  private static Logger LOGGER = LoggerFactory.getLogger(SubmarineUI.class);

  public static String unifyKey(String key) {
    key = key.replace(".", "_").toUpperCase();
    return key;
  }

  // yarn application match the pattern [a-z][a-z0-9-]*
  public static String getJobName(String noteId) {
    return "submarine-" + noteId.toLowerCase();
  }

  public static String getAngularObjectValue(InterpreterContext context, String name) {
    String value = "";
    AngularObject angularObject = context.getAngularObjectRegistry()
        .get(name, context.getNoteId(), context.getParagraphId());
    if (null != angularObject && null != angularObject.get()) {
      value = angularObject.get().toString();
    }
    return value;
  }

  public static void setAngularObjectValue(InterpreterContext context, String name, Object value) {
    AngularObject angularObject = context.getAngularObjectRegistry()
        .add(name, value, context.getNoteId(), context.getParagraphId(), true);
  }

  public static void removeAngularObjectValue(InterpreterContext context, String name) {
    context.getAngularObjectRegistry().remove(name, context.getNoteId(),
        context.getParagraphId(), false);
  }

  // Convert properties to Map and check that the variable cannot be empty
  public static HashMap propertiesToJinjaParams(Properties properties, SubmarineUI submarineUI,
                                                HDFSUtils hdfsUtils, String noteId,
                                                boolean outputLog)
      throws IOException {
    StringBuffer sbMessage = new StringBuffer();

    // Check user-set job variables
    String inputPath = getProperty(
        properties, SubmarineConstants.INPUT_PATH, outputLog, sbMessage);
    String checkPointPath = getProperty(
        properties, SubmarineConstants.CHECKPOINT_PATH, outputLog, sbMessage);
    String psLaunchCmd = getProperty(
        properties, SubmarineConstants.PS_LAUNCH_CMD, outputLog, sbMessage);

    String workerLaunchCmd = getProperty(
        properties, SubmarineConstants.WORKER_LAUNCH_CMD, outputLog, sbMessage);

    // Check interpretere set Properties
    String submarineHadoopHome;
    submarineHadoopHome = getProperty(
        properties, SubmarineConstants.SUBMARINE_HADOOP_HOME, outputLog, sbMessage);
    File file = new File(submarineHadoopHome);
    if (!file.exists()) {
      sbMessage.append(SubmarineConstants.SUBMARINE_HADOOP_HOME + ": "
          + submarineHadoopHome + " is not a valid file path!\n");
    }

    String submarineJar = getProperty(
        properties, SubmarineConstants.HADOOP_YARN_SUBMARINE_JAR, outputLog, sbMessage);
    file = new File(submarineJar);
    if (!file.exists()) {
      sbMessage.append(SubmarineConstants.HADOOP_YARN_SUBMARINE_JAR + ":"
          + submarineJar + " is not a valid file path!\n");
    }
    String containerNetwork = getProperty(
        properties, SubmarineConstants.DOCKER_CONTAINER_NETWORK, outputLog, sbMessage);
    String parameterServicesImage = getProperty(
        properties, SubmarineConstants.TF_PARAMETER_SERVICES_DOCKER_IMAGE, outputLog, sbMessage);
    String parameterServicesNum = getProperty(
        properties, SubmarineConstants.TF_PARAMETER_SERVICES_NUM, outputLog, sbMessage);
    String parameterServicesGpu = getProperty(
        properties, SubmarineConstants.TF_PARAMETER_SERVICES_GPU, outputLog, sbMessage);
    String parameterServicesCpu = getProperty(
        properties, SubmarineConstants.TF_PARAMETER_SERVICES_CPU, outputLog, sbMessage);
    String parameterServicesMemory = getProperty(
        properties, SubmarineConstants.TF_PARAMETER_SERVICES_MEMORY, outputLog, sbMessage);
    String workerServicesImage = getProperty(
        properties, SubmarineConstants.TF_WORKER_SERVICES_DOCKER_IMAGE, outputLog, sbMessage);
    String workerServicesNum = getProperty(
        properties, SubmarineConstants.TF_WORKER_SERVICES_NUM, outputLog, sbMessage);
    String workerServicesGpu = getProperty(
        properties, SubmarineConstants.TF_WORKER_SERVICES_GPU, outputLog, sbMessage);
    String workerServicesCpu = getProperty(
        properties, SubmarineConstants.TF_WORKER_SERVICES_CPU, outputLog, sbMessage);
    String workerServicesMemory = getProperty(
        properties, SubmarineConstants.TF_WORKER_SERVICES_MEMORY, outputLog, sbMessage);
    String algorithmUploadPath = getProperty(
        properties, SubmarineConstants.SUBMARINE_ALGORITHM_HDFS_PATH, outputLog, sbMessage);
    String submarineHadoopKeytab = getProperty(
        properties, SubmarineConstants.SUBMARINE_HADOOP_KEYTAB, outputLog, sbMessage);
    file = new File(submarineHadoopKeytab);
    if (!file.exists()) {
      sbMessage.append(SubmarineConstants.SUBMARINE_HADOOP_KEYTAB + ":"
          + submarineHadoopKeytab + " is not a valid file path!\n");
    }
    String submarineHadoopPrincipal = getProperty(
        properties, SubmarineConstants.SUBMARINE_HADOOP_PRINCIPAL, outputLog, sbMessage);
    String machinelearingDistributed = getProperty(
        properties, SubmarineConstants.MACHINELEARING_DISTRIBUTED_ENABLE, outputLog, sbMessage);
    if (StringUtils.isEmpty(machinelearingDistributed)) {
      machinelearingDistributed = "false"; // default
    }
    String dockerHadoopHdfsHome = getProperty(
        properties, SubmarineConstants.DOCKER_HADOOP_HDFS_HOME, outputLog, sbMessage);
    String dockerJavaHome = getProperty(
        properties, SubmarineConstants.DOCKER_JAVA_HOME, outputLog, sbMessage);
    String intpLaunchMode = getProperty(
        properties, SubmarineConstants.INTERPRETER_LAUNCH_MODE, outputLog, sbMessage);
    if (StringUtils.isEmpty(intpLaunchMode)) {
      intpLaunchMode = "local"; // default
    }
    String sumbarineHadoopConfDir = getProperty(
        properties, SubmarineConstants.SUBMARINE_HADOOP_CONF_DIR, outputLog, sbMessage);

    String notePath = algorithmUploadPath + File.separator + noteId;
    List<String> arrayHdfsFiles = new ArrayList<>();
    List<Path> hdfsFiles = hdfsUtils.list(new Path(notePath + "/*"));
    if (hdfsFiles.size() == 0) {
      sbMessage.append("EXECUTE_SUBMARINE_ERROR: The " + notePath
          + " file directory was is empty in HDFS!\n");
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
        submarineUI.outputLog("Execution information",
            sbCommitFiles.toString());
      }
    }

    // Found null variable, throw exception
    if (!StringUtils.isEmpty(sbMessage.toString()) && outputLog) {
      throw new RuntimeException(sbMessage.toString());
    }

    // Save user-set variables and interpreter configuration parameters
    String jobName = SubmarineUtils.getJobName(noteId);
    HashMap<String, Object> mapParams = new HashMap();
    mapParams.put(unifyKey(SubmarineConstants.INTERPRETER_LAUNCH_MODE), intpLaunchMode);
    mapParams.put(unifyKey(SubmarineConstants.SUBMARINE_HADOOP_HOME), submarineHadoopHome);
    mapParams.put(unifyKey(SubmarineConstants.SUBMARINE_HADOOP_CONF_DIR), sumbarineHadoopConfDir);
    mapParams.put(unifyKey(SubmarineConstants.DOCKER_HADOOP_HDFS_HOME), dockerHadoopHdfsHome);
    mapParams.put(unifyKey(SubmarineConstants.DOCKER_JAVA_HOME), dockerJavaHome);
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

  private static String getProperty(Properties properties, String key,
                                    boolean outputLog, StringBuffer sbMessage) {
    String value = properties.getProperty(key, "");
    if (StringUtils.isEmpty(value) && outputLog) {
      sbMessage.append("EXECUTE_SUBMARINE_ERROR: " +
          "Please set the submarine interpreter properties : ");
      sbMessage.append(key).append("\n");
    }

    return value;
  }
}
