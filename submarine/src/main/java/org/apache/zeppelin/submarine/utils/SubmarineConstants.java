/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. See accompanying LICENSE file.
 */

package org.apache.zeppelin.submarine.utils;

/*
 * NOTE: use lowercase + "_" for the option name
 */
public class SubmarineConstants {
  public static final String HADOOP_HOME = "HADOOP_HOME";
  public static final String DOCKER_JAVA_HOME = "DOCKER_JAVA_HOME";
  public static final String DOCKER_HADOOP_HDFS_HOME = "DOCKER_HADOOP_HDFS_HOME";

//  public static final String CONTAINER_NETWORK = "CONTAINER_NETWORK";

  public static final String JOB_NAME = "JOB_NAME";
  public static final String ALGORITHM_FILE_FULL_PATH = "ALGORITHM_FILE_FULL_PATH";
  public static final String INPUT_PATH = "INPUT_PATH";
  public static final String CHECKPOINT_PATH = "CHECKPOINT_PATH";
  public static final String PS_LAUNCH_CMD = "PS_LAUNCH_CMD";
  public static final String WORKER_LAUNCH_CMD = "WORKER_LAUNCH_CMD";

  public static final String HELP_COMMAND = "help";

  public static final String HADOOP_CONF_DIR = "HADOOP_CONF_DIR";
  public static final String HADOOP_YARN_SUBMARINE_JAR = "hadoop.yarn.submarine.jar";
  public static final String SUBMARINE_YARN_QUEUE      = "submarine.yarn.queue";
  public static final String DOCKER_CONTAINER_NETWORK  = "docker.container.network";
  public static final String SUBMARINE_CONCURRENT_MAX = "submarine.concurrent.max";

  public static final String SUBMARINE_HDFS_KEYTAB    = "submarine.hdfs.keytab";
  public static final String SUBMARINE_HDFS_PRINCIPAL = "submarine.hdfs.principal";

  public static final String ALGORITHM_UPLOAD_PATH    = "algorithm.upload.path";


  public static final String PARAMETER_SERVICES_DOCKER_IMAGE = "parameter.services.docker.image";
  public static final String PARAMETER_SERVICES_NUM = "parameter.services.num";
  public static final String PARAMETER_SERVICES_GPU = "parameter.services.gpu";
  public static final String PARAMETER_SERVICES_CPU = "parameter.services.cpu";
  public static final String PARAMETER_SERVICES_MEMORY = "parameter.services.memory";

  public static final String WORKER_SERVICES_DOCKER_IMAGE = "worker.services.docker.image";
  public static final String WORKER_SERVICES_NUM = "worker.services.num";
  public static final String WORKER_SERVICES_GPU = "worker.services.gpu";
  public static final String WORKER_SERVICES_CPU = "worker.services.cpu";
  public static final String WORKER_SERVICES_MEMORY = "worker.services.memory";

  public static final String TENSORBOARD_ENABLE  = "tensorboard.enable";

  public static final String COMMAND_JOB_LIST = "job list";
  public static final String COMMAND_JOB_SHOW = "job show";
  public static final String COMMAND_JOB_RUN  = "job run";

  public static final String NOTE_ID = "NOTE_ID";
  public static final String NOTE_NAME = "NOTE_NAME";
  public static final String PARAGRAPH_ID = "PARAGRAPH_ID";
  public static final String PARAGRAPH_TITLE = "PARAGRAPH_TITLE";
  public static final String PARAGRAPH_TEXT = "PARAGRAPH_TEXT";
  public static final String REPL_NAME = "REPL_NAME";
  public static final String SCRIPT = "SCRIPT";

}
