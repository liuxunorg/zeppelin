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

package org.apache.zeppelin.interpreter.launcher;

import com.google.common.io.Resources;
import com.hubspot.jinjava.Jinjava;
import org.apache.commons.io.Charsets;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URL;
import java.util.HashMap;

import static org.junit.Assert.assertEquals;

public class JinjaTemplatesTest {
  private static Logger LOGGER = LoggerFactory.getLogger(JinjaTemplatesTest.class);

  @Test
  public void jobRunJinjaTemplateTest9() throws IOException {
    String str = jobRunJinjaTemplateTest(Boolean.TRUE, Boolean.FALSE);
    StringBuffer sbCheck = new StringBuffer();
    sbCheck.append("SUBMARINE_HADOOP_HOME_VALUE/bin/yarn jar \\\n" +
        "  HADOOP_YARN_SUBMARINE_JAR_VALUE job run \\\n" +
        "  --name JOB_NAME_VALUE \\\n" +
        "  --env DOCKER_JAVA_HOME=DOCKER_JAVA_HOME_VALUE \\\n" +
        "  --env DOCKER_HADOOP_HDFS_HOME=DOCKER_HADOOP_HDFS_HOME_VALUE \\\n" +
        "  --env YARN_CONTAINER_RUNTIME_DOCKER_CONTAINER_NETWORK=bridge \\\n" +
        "  --env TZ=\"\" \\\n" +
        "  --checkpoint_path CHECKPOINT_PATH_VALUE \\\n" +
        "  --queue SUBMARINE_YARN_QUEUE \\\n" +
        "  --num_workers 0 \\\n" +
        "  --tensorboard \\\n" +
        "  --tensorboard_docker_image TF_PARAMETER_SERVICES_DOCKER_IMAGE_VALUE \\\n" +
        "  --keytab SUBMARINE_HADOOP_KEYTAB_VALUE \\\n" +
        "  --principal SUBMARINE_HADOOP_PRINCIPAL_VALUE \\\n" +
        "  --distribute_keytab \\\n" +
        "  --verbose");
    assertEquals(str, sbCheck.toString());
  }

  public String jobRunJinjaTemplateTest(Boolean dist, Boolean launchMode) throws IOException {
    URL urlTemplate = Resources.getResource("jinja_templates/yarn-submit.jinja");
    String template = Resources.toString(urlTemplate, Charsets.UTF_8);
    Jinjava jinjava = new Jinjava();
    HashMap<String, Object> jinjaParams = new HashMap();

    String yarnSubmit = jinjava.render(template, jinjaParams);
    int pos = yarnSubmit.indexOf("\n");
    if (pos == 0) {
      yarnSubmit = yarnSubmit.replaceFirst("\n", "");
    }

    LOGGER.info("------------------------");
    LOGGER.info(yarnSubmit);
    LOGGER.info("------------------------");

    yarnSubmit = yarnSubmit.replaceAll("\n", "");
    String[] yarnSubmitArgs = yarnSubmit.split("\\\\");

    return yarnSubmit;
  }
}
