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

/*
 * NOTE: use lowercase + "_" for the option name
 */
public class SubmarineConstants {
  public static final String SUBMARINE_HADOOP_HOME  = "submarine.hadoop.home";
  public static final String DOCKER_JAVA_HOME       = "DOCKER_JAVA_HOME";
  public static final String DOCKER_HADOOP_HDFS_HOME = "DOCKER_HADOOP_HDFS_HOME";

  public static final String JOB_NAME = "JOB_NAME";
  public static final String TF_INPUT_PATH = "TF_INPUT_PATH";
  public static final String TF_CHECKPOINT_PATH = "TF_CHECKPOINT_PATH";
  public static final String TF_PS_LAUNCH_CMD = "TF_PS_LAUNCH_CMD";
  public static final String TF_WORKER_LAUNCH_CMD = "TF_WORKER_LAUNCH_CMD";

  public static final String HELP_COMMAND = "help";

  public static final String HADOOP_YARN_SUBMARINE_JAR = "hadoop.yarn.submarine.jar";
  public static final String DOCKER_CONTAINER_NETWORK   = "docker.container.network";
  public static final String SUBMARINE_YARN_QUEUE       = "submarine.yarn.queue";
  public static final String SUBMARINE_CONCURRENT_MAX   = "submarine.concurrent.max";
  public static final String SUBMARINE_KRB5_CONF        = "submarine.krb5.conf";
  public static final String SUBMARINE_HADOOP_KEYTAB    = "submarine.hadoop.keytab";
  public static final String SUBMARINE_HADOOP_PRINCIPAL = "submarine.hadoop.principal";

  public static final String SUBMARINE_ALGORITHM_HDFS_PATH = "submarine.algorithm.hdfs.path";

  public static final String TF_PARAMETER_SERVICES_DOCKER_IMAGE
      = "tf.parameter.services.docker.image";
  public static final String TF_PARAMETER_SERVICES_NUM = "tf.parameter.services.num";
  public static final String TF_PARAMETER_SERVICES_GPU = "tf.parameter.services.gpu";
  public static final String TF_PARAMETER_SERVICES_CPU = "tf.parameter.services.cpu";
  public static final String TF_PARAMETER_SERVICES_MEMORY = "tf.parameter.services.memory";

  public static final String TF_WORKER_SERVICES_DOCKER_IMAGE = "tf.worker.services.docker.image";
  public static final String TF_WORKER_SERVICES_NUM = "tf.worker.services.num";
  public static final String TF_WORKER_SERVICES_GPU = "tf.worker.services.gpu";
  public static final String TF_WORKER_SERVICES_CPU = "tf.worker.services.cpu";
  public static final String TF_WORKER_SERVICES_MEMORY = "tf.worker.services.memory";

  public static final String TF_TENSORBOARD_ENABLE  = "tf.tensorboard.enable";

  public static final String COMMAND_JOB_LIST = "job list";
  public static final String COMMAND_JOB_SHOW = "job show";
  public static final String COMMAND_JOB_RUN  = "job run";

  public static final String NOTE_ID = "NOTE_ID";
  public static final String NOTE_NAME = "NOTE_NAME";
  public static final String PARAGRAPH_ID = "PARAGRAPH_ID";
  public static final String PARAGRAPH_TEXT = "PARAGRAPH_TEXT";
  public static final String REPL_NAME = "REPL_NAME";
  public static final String SCRIPT = "SCRIPT";

}
