package org.apache.zeppelin.submarine;

import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import org.apache.zeppelin.conf.ZeppelinConfiguration;
import org.apache.zeppelin.submarine.componts.YarnClient;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class YarnClientTest {
  private static Logger LOGGER = LoggerFactory.getLogger(YarnClientTest.class);

  private static YarnClient yarnClient = null;

  @BeforeClass
  public static void initEnv() {
    ZeppelinConfiguration zconf = ZeppelinConfiguration.create();
    Properties properties = new Properties();
    yarnClient = new YarnClient(properties);
  }

  @Test
  public void testParseAppAttempts() throws IOException {
    String jsonFile = "appAttempts.json";
    URL urlJson = Resources.getResource(jsonFile);
    String jsonContent = Resources.toString(urlJson, Charsets.UTF_8);

    List<Map<String, Object>> list = yarnClient.parseAppAttempts(jsonContent);

    LOGGER.info("");
  }

  @Test
  public void testParseAppAttemptsContainers() throws IOException {
    String jsonFile = "appattempts-containers.json";
    URL urlJson = Resources.getResource(jsonFile);
    String jsonContent = Resources.toString(urlJson, Charsets.UTF_8);

    List<Map<String, Object>> list = yarnClient.parseAppAttemptsContainers(jsonContent);

    list.get(0).get(YarnClient.HOST_IP);
    list.get(0).get(YarnClient.HOST_PORT);
    list.get(0).get(YarnClient.CONTAINER_PORT);

    LOGGER.info("");
  }

  @Test
  public void testParseClusterApps() throws IOException {
    String jsonFile = "clusterApps.json";
    URL urlJson = Resources.getResource(jsonFile);
    String jsonContent = Resources.toString(urlJson, Charsets.UTF_8);

    Map<String, Object> list = yarnClient.parseClusterApps(jsonContent);

    LOGGER.info("");
  }
}