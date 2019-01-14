package org.apache.zeppelin.submarine;

import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import org.apache.zeppelin.conf.ZeppelinConfiguration;
import org.apache.zeppelin.submarine.utils.YarnRestClient;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class YarnRestClientTest {
  private static Logger LOGGER = LoggerFactory.getLogger(YarnRestClientTest.class);

  private static YarnRestClient yarnRestClient = null;

  @BeforeClass
  public static void initEnv() {
    ZeppelinConfiguration zconf = ZeppelinConfiguration.create();
    String uri = zconf.getDocument().getDocumentURI();
    Properties properties = new Properties();
    yarnRestClient = new YarnRestClient(properties);
  }

  @Test
  public void testParseAppAttempts() throws IOException {
    String jsonFile = "appAttempts.json";
    URL urlJson = Resources.getResource(jsonFile);
    String jsonContent = Resources.toString(urlJson, Charsets.UTF_8);

    List<Map<String, Object>> list = yarnRestClient.parseAppAttempts(jsonContent);

    LOGGER.info("");
  }

  @Test
  public void testParseAppAttemptsContainers() throws IOException {
    String jsonFile = "appattempts-containers.json";
    URL urlJson = Resources.getResource(jsonFile);
    String jsonContent = Resources.toString(urlJson, Charsets.UTF_8);

    List<Map<String, Object>> list = yarnRestClient.parseAppAttemptsContainers(jsonContent);

    list.get(0).get(YarnRestClient.HOST_IP);
    list.get(0).get(YarnRestClient.HOST_PORT);
    list.get(0).get(YarnRestClient.CONTAINER_PORT);

    LOGGER.info("");
  }
}
