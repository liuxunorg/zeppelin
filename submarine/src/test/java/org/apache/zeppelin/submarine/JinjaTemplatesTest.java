package org.apache.zeppelin.submarine;

import com.google.common.io.Resources;
import com.hubspot.jinjava.Jinjava;
import org.apache.commons.io.Charsets;
import org.apache.zeppelin.submarine.utils.SubmarineConstants;
import org.apache.zeppelin.submarine.utils.SubmarineJob;
import org.apache.zeppelin.submarine.utils.SubmarineUtils;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class JinjaTemplatesTest {
  private static Logger LOGGER = LoggerFactory.getLogger(JinjaTemplatesTest.class);

  @Test
  public void jobRunJinjaTemplateTest() throws IOException {
    jobRunJinjaTemplateTest(Boolean.TRUE, Boolean.TRUE);
    jobRunJinjaTemplateTest(Boolean.TRUE, Boolean.FALSE);
    jobRunJinjaTemplateTest(Boolean.TRUE, null);
    jobRunJinjaTemplateTest(Boolean.FALSE, Boolean.TRUE);
    jobRunJinjaTemplateTest(Boolean.FALSE, Boolean.FALSE);
    jobRunJinjaTemplateTest(null, Boolean.FALSE);
    jobRunJinjaTemplateTest(null, null);
  }

  public void jobRunJinjaTemplateTest(Boolean dist, Boolean launchMode) throws IOException {
    URL urlTemplate = Resources.getResource(SubmarineJob.SUBMARINE_JOBRUN_TF_JINJA);
    String template = Resources.toString(urlTemplate, Charsets.UTF_8);
    Jinjava jinjava = new Jinjava();
    HashMap<String, Object> jinjaParams = new HashMap();

    if (launchMode == Boolean.TRUE) {
      jinjaParams.put(SubmarineUtils.unifyKey(SubmarineConstants.INTERPRETER_LAUNCH_MODE), "yarn");
    } else if (launchMode == Boolean.FALSE) {
      jinjaParams.put(SubmarineUtils.unifyKey(SubmarineConstants.INTERPRETER_LAUNCH_MODE), "local");
    }

    if (dist == Boolean.TRUE) {
      jinjaParams.put(SubmarineUtils.unifyKey(
          SubmarineConstants.MACHINELEARING_DISTRIBUTED_ENABLE), "true");
    } else if (dist == Boolean.FALSE) {
      jinjaParams.put(SubmarineUtils.unifyKey(
          SubmarineConstants.MACHINELEARING_DISTRIBUTED_ENABLE), "false");
    }

    List<String> arrayHdfsFiles = new ArrayList<>();
    arrayHdfsFiles.add("hdfs://file1");
    arrayHdfsFiles.add("hdfs://file2");
    arrayHdfsFiles.add("hdfs://file3");
    jinjaParams.put(SubmarineUtils.unifyKey(
        SubmarineConstants.SUBMARINE_ALGORITHM_HDFS_FILES), arrayHdfsFiles);

    // mock
    jinjaParams.put(SubmarineUtils.unifyKey(
        SubmarineConstants.DOCKER_HADOOP_HDFS_HOME), "DOCKER_HADOOP_HDFS_HOME_VALUE");
    jinjaParams.put(SubmarineUtils.unifyKey(
        SubmarineConstants.JOB_NAME), "JOB_NAME_VALUE");
    jinjaParams.put(SubmarineUtils.unifyKey(
        SubmarineConstants.HADOOP_YARN_SUBMARINE_JAR), "HADOOP_YARN_SUBMARINE_JAR_VALUE");
    jinjaParams.put(SubmarineUtils.unifyKey(
        SubmarineConstants.SUBMARINE_HADOOP_HOME), "SUBMARINE_HADOOP_HOME_VALUE");
    jinjaParams.put(SubmarineUtils.unifyKey(
        SubmarineConstants.DOCKER_JAVA_HOME), "DOCKER_JAVA_HOME_VALUE");
    jinjaParams.put(SubmarineUtils.unifyKey(
        SubmarineConstants.DOCKER_CONTAINER_NETWORK), "DOCKER_CONTAINER_NETWORK_VALUE");
    jinjaParams.put(SubmarineUtils.unifyKey(
        SubmarineConstants.INPUT_PATH), "INPUT_PATH_VALUE");
    jinjaParams.put(SubmarineUtils.unifyKey(
        SubmarineConstants.CHECKPOINT_PATH), "CHECKPOINT_PATH_VALUE");
    jinjaParams.put(SubmarineUtils.unifyKey(
        SubmarineConstants.TF_PARAMETER_SERVICES_NUM), "TF_PARAMETER_SERVICES_NUM_VALUE");
    jinjaParams.put(SubmarineUtils.unifyKey(
        SubmarineConstants.TF_PARAMETER_SERVICES_DOCKER_IMAGE),
        "TF_PARAMETER_SERVICES_DOCKER_IMAGE_VALUE");
    jinjaParams.put(SubmarineUtils.unifyKey(
        SubmarineConstants.TF_PARAMETER_SERVICES_MEMORY), "TF_PARAMETER_SERVICES_MEMORY_VALUE");
    jinjaParams.put(SubmarineUtils.unifyKey(
        SubmarineConstants.TF_PARAMETER_SERVICES_CPU), "TF_PARAMETER_SERVICES_CPU_VALUE");
    jinjaParams.put(SubmarineUtils.unifyKey(
        SubmarineConstants.TF_PARAMETER_SERVICES_GPU), "TF_PARAMETER_SERVICES_GPU_VALUE");
    jinjaParams.put(SubmarineUtils.unifyKey(
        SubmarineConstants.PS_LAUNCH_CMD), "PS_LAUNCH_CMD_VALUE");
    jinjaParams.put(SubmarineUtils.unifyKey(
        SubmarineConstants.TF_WORKER_SERVICES_DOCKER_IMAGE),
        "TF_WORKER_SERVICES_DOCKER_IMAGE_VALUE");
    jinjaParams.put(SubmarineUtils.unifyKey(
        SubmarineConstants.TF_WORKER_SERVICES_NUM), "TF_WORKER_SERVICES_NUM_VALUE");
    jinjaParams.put(SubmarineUtils.unifyKey(
        SubmarineConstants.TF_WORKER_SERVICES_DOCKER_IMAGE),
        "TF_WORKER_SERVICES_DOCKER_IMAGE_VALUE");
    jinjaParams.put(SubmarineUtils.unifyKey(
        SubmarineConstants.TF_WORKER_SERVICES_MEMORY), "TF_WORKER_SERVICES_MEMORY_VALUE");
    jinjaParams.put(SubmarineUtils.unifyKey(
        SubmarineConstants.TF_WORKER_SERVICES_CPU), "TF_WORKER_SERVICES_CPU_VALUE");
    jinjaParams.put(SubmarineUtils.unifyKey(
        SubmarineConstants.TF_WORKER_SERVICES_GPU), "TF_WORKER_SERVICES_GPU_VALUE");
    jinjaParams.put(SubmarineUtils.unifyKey(
        SubmarineConstants.WORKER_LAUNCH_CMD), "WORKER_LAUNCH_CMD_VALUE");
    jinjaParams.put(SubmarineUtils.unifyKey(
        SubmarineConstants.SUBMARINE_ALGORITHM_HDFS_PATH), "SUBMARINE_ALGORITHM_HDFS_PATH_VALUE");
    jinjaParams.put(SubmarineUtils.unifyKey(
        SubmarineConstants.SUBMARINE_HADOOP_KEYTAB), "SUBMARINE_HADOOP_KEYTAB_VALUE");
    jinjaParams.put(SubmarineUtils.unifyKey(
        SubmarineConstants.SUBMARINE_HADOOP_PRINCIPAL), "SUBMARINE_HADOOP_PRINCIPAL_VALUE");

    String submarineCmd = jinjava.render(template, jinjaParams);
    LOGGER.info("------------------------");
    LOGGER.info(submarineCmd);
    LOGGER.info("------------------------");
  }
}
