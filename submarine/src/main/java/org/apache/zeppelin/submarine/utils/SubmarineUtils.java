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
    // Properties properties = submarineJob.getProperties();

    String submarineHadoopHome;
    String submarineJar;
    submarineHadoopHome = properties.getProperty(SubmarineConstants.SUBMARINE_HADOOP_HOME, "");
    submarineJar = properties.getProperty(SubmarineConstants.HADOOP_YARN_SUBMARINE_JAR, "");

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
    String algorithmUploadPath = properties.getProperty(
        SubmarineConstants.SUBMARINE_ALGORITHM_HDFS_PATH, "");
    if (StringUtils.isEmpty(algorithmUploadPath) && outputLog) {
      setIntpPropertiesWarn(sbMessage, SubmarineConstants.SUBMARINE_ALGORITHM_HDFS_PATH);
    }
    String submarineHadoopKeytab = properties.getProperty(
        SubmarineConstants.SUBMARINE_HADOOP_KEYTAB, "");
    if (StringUtils.isEmpty(submarineHadoopKeytab) && outputLog) {
      setIntpPropertiesWarn(sbMessage, SubmarineConstants.SUBMARINE_HADOOP_KEYTAB);
    }
    file = new File(submarineHadoopKeytab);
    if (!file.exists()) {
      sbMessage.append(SubmarineConstants.SUBMARINE_HADOOP_KEYTAB + ":"
          + submarineHadoopKeytab + " is not a valid file path!\n");
    }
    String submarineHadoopPrincipal = properties.getProperty(
        SubmarineConstants.SUBMARINE_HADOOP_PRINCIPAL, "");
    if (StringUtils.isEmpty(submarineHadoopKeytab) && outputLog) {
      setIntpPropertiesWarn(sbMessage, SubmarineConstants.SUBMARINE_HADOOP_PRINCIPAL);
    }
    String machinelearingDistributed = properties.getProperty(
        SubmarineConstants.MACHINELEARING_DISTRIBUTED_ENABLE, "false");

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

  private static StringBuffer setUserPropertiesWarn(
      StringBuffer sbMessage, String key, String info) {
    sbMessage.append("EXECUTE_SUBMARINE_ERROR: Please set the parameter ");
    sbMessage.append(key);
    sbMessage.append(" first, \nfor example: ");
    sbMessage.append(key + info);

    return sbMessage;
  }

  private static void setIntpPropertiesWarn(StringBuffer sbMessage, String key) {
    sbMessage.append("EXECUTE_SUBMARINE_ERROR: Please set the submarine interpreter properties : ");
    sbMessage.append(key).append("\n");
  }
}
