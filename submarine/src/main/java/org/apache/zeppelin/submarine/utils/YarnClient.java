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

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import com.google.gson.JsonElement;
import com.google.gson.JsonIOException;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonSyntaxException;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.client.api.AppAdminClient;
import org.apache.hadoop.yarn.exceptions.ApplicationNotFoundException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class YarnClient {
  private Logger LOGGER = LoggerFactory.getLogger(YarnClient.class);

  private Configuration hadoopConf;

  private Properties properties;

  public static final String APPLICATION_ID     = "APPLICATION_ID";
  public static final String APPLICATION_NAME   = "APPLICATION_NAME";
  public static final String APPLICATION_STATUS = "APPLICATION_STATUS";

  public static final String APPLICATION_STATUS_ACCEPT    = "Accept";
  public static final String APPLICATION_STATUS_RUNNING   = "Running";
  public static final String APPLICATION_STATUS_FINISHED  = "Finished";
  public static final String APPLICATION_STATUS_FAILED    = "Failed";

  public YarnClient(Properties properties) {
    this.hadoopConf = new Configuration();

    this.properties = properties;
  }

  public Map<String, Object> getAppStatus(String appIdOrName) {
    AppAdminClient client = AppAdminClient.createAppAdminClient(
        AppAdminClient.DEFAULT_TYPE, hadoopConf);

    String appStatus = "";
    try {
      appStatus = client.getStatusString(appIdOrName);
    } catch (ApplicationNotFoundException exception) {
      LOGGER.error("Application with name '" + appIdOrName
          + "' doesn't exist in RM or Timeline Server.");
    } catch (Exception ie) {
      System.err.println(ie.getMessage());
    }

    // parse app status json
    Map<String, Object> mapStatus = parseAppStatus(appStatus);

    return mapStatus;
  }

  private Map<String, Object> parseAppStatus(String appJson) {
    Map<String, Object> mapStatus = new HashMap<>();

    try {
      JsonParser jsonParser = new JsonParser();
      JsonObject jsonObject = (JsonObject) jsonParser.parse(appJson);

      JsonElement elementAppId = jsonObject.get("id");
      JsonElement elementAppState = jsonObject.get("state");
      JsonElement elementAppName = jsonObject.get("name");

      String appId = (elementAppId == null) ? "" : elementAppId.getAsString();
      String appState = (elementAppState == null) ? "" : elementAppState.getAsString();
      String appName = (elementAppName == null) ? "" : elementAppName.getAsString();

      if (!StringUtils.isEmpty(appId)) {
        mapStatus.put(APPLICATION_ID, appId);
      }
      if (!StringUtils.isEmpty(appName)) {
        mapStatus.put(APPLICATION_NAME, appName);
      }
      if (!StringUtils.isEmpty(appState)) {
        mapStatus.put(APPLICATION_STATUS, appState);
      }
    } catch (JsonIOException e) {
      e.printStackTrace();
    } catch (JsonSyntaxException e) {
      e.printStackTrace();
    }

    return mapStatus;
  }
}
