/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.zeppelin.interpreter.launcher;

import org.apache.commons.lang.StringUtils;
import org.apache.zeppelin.conf.ZeppelinConfiguration;
import org.apache.zeppelin.interpreter.Constants;
import org.apache.zeppelin.interpreter.InterpreterOption;
import org.apache.zeppelin.interpreter.InterpreterRunner;
import org.apache.zeppelin.interpreter.recovery.RecoveryStorage;
import org.apache.zeppelin.interpreter.remote.RemoteInterpreterManagedProcess;
import org.apache.zeppelin.interpreter.remote.RemoteInterpreterRunningProcess;
import org.apache.zeppelin.submarine.utils.SubmarineConstants;
import org.apache.zeppelin.submarine.utils.YarnRestClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * Submarine specific launcher.
 */
public class SubmarineInterpreterLauncher extends StandardInterpreterLauncher {
  private static final Logger LOGGER = LoggerFactory.getLogger(SubmarineInterpreterLauncher.class);

  private YarnRestClient yarnRestClient = null;

  public static final String CONTAINER_ZEPPELIN_CONF_DIR = "/submarine/zeppelin";

  public SubmarineInterpreterLauncher(ZeppelinConfiguration zConf, RecoveryStorage recoveryStorage) {
    super(zConf, recoveryStorage);
  }

  private InterpreterClient launchOnYarn(InterpreterLaunchContext context) throws IOException {
    this.properties = context.getProperties();
    yarnRestClient = new YarnRestClient(properties);

    InterpreterOption option = context.getOption();
    InterpreterRunner runner = context.getRunner();
    String groupName = context.getInterpreterSettingGroup();
    String name = context.getInterpreterSettingName();
    int connectTimeout = getConnectTimeout();

    if (option.isExistingProcess()) {
      return new RemoteInterpreterRunningProcess(
          context.getInterpreterSettingName(),
          connectTimeout,
          option.getHost(),
          option.getPort());
    }

    // yarn application match the pattern [a-z][a-z0-9-]*
    String submarineIntpAppName = context.getInterpreterGroupId().toLowerCase().replace("_", "-");
    properties.put("SUBMARINE_JOB_NAME", submarineIntpAppName);

    // setting port range of submarine interpreter container
    String intpPort = properties.getProperty(SubmarineConstants.ZEPPELIN_INTERPRETER_RPC_PORTRANGE,
        String.valueOf(Constants.ZEPPELIN_INTERPRETER_DEFAUlT_PORT));
    String intpPortRange = intpPort + ":" + intpPort;

    // upload configure file to submarine interpreter container
    // keytab file & zeppelin-site.xml & krb5.conf
    // The submarine configures the mount file into the container through `localization`
    String keytab = properties.getProperty(SubmarineConstants.SUBMARINE_HADOOP_KEYTAB, "");
    String zconfFile = zConf.getDocument().getDocumentURI();
    if (zconfFile.startsWith("file:")) {
      zconfFile = zconfFile.replace("file:", "");
    }
    String krb5File = properties.getProperty(SubmarineConstants.SUBMARINE_HADOOP_KRB5_CONF, "");
    StringBuffer sbLocalization = new StringBuffer();
    if (!StringUtils.isEmpty(keytab)) {
      sbLocalization.append("--localization \"");
      sbLocalization.append(keytab + ":" + keytab + ":rw\"").append(" ");
    }
    if (!StringUtils.isEmpty(krb5File)) {
      sbLocalization.append("--localization \"");
      sbLocalization.append(krb5File + ":" + krb5File + ":rw\"").append(" ");
    }
    if (!StringUtils.isEmpty(zconfFile)) {
      // zeppelin-site.xml is uploaded to a specific `/submarine` directory in the container
      sbLocalization.append("--localization \"");
      sbLocalization.append(zconfFile + ":" + CONTAINER_ZEPPELIN_CONF_DIR);
      sbLocalization.append("/zeppelin-site.xml:rw\"").append(" ");
    }
    properties.put("SUBMARINE_LOCALIZATION", sbLocalization.toString());
    properties.put("SUBMARINE_ZEPPELIN_CONF_DIR_EVN", "--env ZEPPELIN_CONF_DIR=" + CONTAINER_ZEPPELIN_CONF_DIR);
    LOGGER.info("SUBMARINE_LOCALIZATION=" + sbLocalization.toString());

    // 1. Query the IP and port of the submarine interpreter process through the yarn client
    List<Map<String, Object>> listExportPorts = detectionSubmarineIntpContainer(submarineIntpAppName);

    String intpAppHostIp = "";
    String intpAppHostPort = "";
    String intpAppContainerPort = "";
    boolean findExistIntpContainer = false;
    for (Map<String, Object> exportPorts : listExportPorts) {
      if (exportPorts.containsKey(YarnRestClient.HOST_IP) && exportPorts.containsKey(YarnRestClient.HOST_PORT)
          && exportPorts.containsKey(YarnRestClient.CONTAINER_PORT)) {
        intpAppHostIp = (String) exportPorts.get(YarnRestClient.HOST_IP);
        intpAppHostPort = (String) exportPorts.get(YarnRestClient.HOST_PORT);
        intpAppContainerPort = (String) exportPorts.get(YarnRestClient.CONTAINER_PORT);
        if (StringUtils.equals(intpPort, intpAppContainerPort)) {
          findExistIntpContainer = true;
          LOGGER.info("Detection Submarine interpreter Container hostIp:{}, hostPort:{}, containerPort:{}.",
              intpAppHostIp, intpAppHostPort, intpAppContainerPort);
          break;
        }
      }
    }

    if (false == findExistIntpContainer) {
      // try to recover it first
      if (zConf.isRecoveryEnabled()) {
        InterpreterClient recoveredClient =
            recoveryStorage.getInterpreterClient(context.getInterpreterGroupId());
        if (recoveredClient != null) {
          if (recoveredClient.isRunning()) {
            LOGGER.info("Recover interpreter process: " + recoveredClient.getHost() + ":" +
                recoveredClient.getPort());
            return recoveredClient;
          } else {
            LOGGER.warn("Cannot recover interpreter process: " + recoveredClient.getHost() + ":"
                + recoveredClient.getPort() + ", as it is already terminated.");
          }
        }
      }

      // 2. Create a submarine interpreter process with hadoop submarine
      String localRepoPath = zConf.getInterpreterLocalRepoPath() + "/"
          + context.getInterpreterSettingId();
      RemoteInterpreterManagedProcess remoteInterpreterManagedProcess = new RemoteInterpreterManagedProcess(
          runner != null ? runner.getPath() : zConf.getInterpreterRemoteRunnerPath(),
          context.getZeppelinServerRPCPort(), context.getZeppelinServerHost(), intpPortRange,
          zConf.getInterpreterDir() + "/" + groupName, localRepoPath,
          buildEnvFromProperties(context), connectTimeout, name,
          context.getInterpreterGroupId(), option.isUserImpersonate());
      try {
        remoteInterpreterManagedProcess.start(context.getUserName());
      } catch (IOException e) {
        LOGGER.error(e.getMessage());
      }

      // 3. Connect to the interpreter process created by YARN
      Date beginDate = new Date();
      Date checkDate = new Date();
      while (checkDate.getTime() - beginDate.getTime() < connectTimeout * 1000) {
        listExportPorts.clear();
        listExportPorts = detectionSubmarineIntpContainer(submarineIntpAppName);

        findExistIntpContainer = false;
        // 2. Create a submarine interpreter process with hadoop submarine
        for (Map<String, Object> exportPorts : listExportPorts) {
          if (exportPorts.containsKey(YarnRestClient.HOST_IP) && exportPorts.containsKey(YarnRestClient.HOST_PORT)
              && exportPorts.containsKey(YarnRestClient.CONTAINER_PORT)) {
            intpAppHostIp = (String) exportPorts.get(YarnRestClient.HOST_IP);
            intpAppHostPort = (String) exportPorts.get(YarnRestClient.HOST_PORT);
            intpAppContainerPort = (String) exportPorts.get(YarnRestClient.CONTAINER_PORT);
            if (StringUtils.equals(intpPort, intpAppContainerPort)) {
              findExistIntpContainer = true;
              break;
            }
          }
        }

        if (false == findExistIntpContainer) {
          try {
            Thread.sleep(1000);
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
        } else {
          LOGGER.info("Detection Submarine interpreter Container hostIp:{}, hostPort:{}, containerPort:{}.",
              intpAppHostIp, intpAppHostPort, intpAppContainerPort);
          break;
        }

        checkDate = new Date();
      }
    }

    return new RemoteInterpreterRunningProcess(
        context.getInterpreterSettingName(),
        connectTimeout,
        intpAppHostIp,
        Integer.parseInt(intpAppHostPort));
  }

  @Override
  public InterpreterClient launch(InterpreterLaunchContext context) throws IOException {
    String launchMode = context.getProperties().getProperty(SubmarineConstants.INTERPRETER_LAUNCH_MODE);
    LOGGER.info("Launching SubmarineInterpreter: "
        + context.getInterpreterSettingGroup() + " on " + launchMode);

    if (StringUtils.equals(launchMode, "yarn")) {
      return launchOnYarn(context);
    } else {
      return super.launch(context);
    }
  }

  private List<Map<String, Object>> detectionSubmarineIntpContainer(String name) {
    // Query the IP and port of the submarine interpreter process through the yarn client
    Map<String, Object> mapAppStatus = yarnRestClient.getAppState(name);
    if (mapAppStatus.containsKey(SubmarineConstants.YARN_APPLICATION_ID)
        && mapAppStatus.containsKey(SubmarineConstants.YARN_APPLICATION_NAME)
        && mapAppStatus.containsKey(SubmarineConstants.YARN_APPLICATION_STATUS)) {
      String appId = mapAppStatus.get(SubmarineConstants.YARN_APPLICATION_ID).toString();
      String appStatus = mapAppStatus.get(SubmarineConstants.YARN_APPLICATION_STATUS).toString();

      // if (StringUtils.equals(appStatus, SubmarineJob.YarnApplicationState.RUNNING.toString())) {
      List<Map<String, Object>> mapAppAttempts = yarnRestClient.getAppAttemptsContainersExportPorts(appId);
      return mapAppAttempts;
      //}
    }

    return new ArrayList<Map<String, Object>>(){};
  }
}
