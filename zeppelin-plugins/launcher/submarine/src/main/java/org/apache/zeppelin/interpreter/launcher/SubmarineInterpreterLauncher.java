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
import org.apache.zeppelin.interpreter.remote.RemoteInterpreterUtils;
import org.apache.zeppelin.submarine.utils.SubmarineConstants;
import org.apache.zeppelin.submarine.utils.YarnRestClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Submarine specific launcher.
 */
public class SubmarineInterpreterLauncher extends StandardInterpreterLauncher {
  private static final Logger LOGGER = LoggerFactory.getLogger(SubmarineInterpreterLauncher.class);

  private YarnRestClient yarnRestClient = null;

  // interpreter.sh
  public static final String CONTAINER_ZEPPELIN_HOME = "/submarine/zeppelin";

  public SubmarineInterpreterLauncher(ZeppelinConfiguration zConf, RecoveryStorage recoveryStorage) {
    super(zConf, recoveryStorage);
  }

  private InterpreterClient launchOnYarn(InterpreterLaunchContext context) throws IOException {
    // Because need to modify the Properties, make a clone
    this.properties = (Properties) context.getProperties().clone();
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

    // yarn application name match the pattern [a-z][a-z0-9-]*
    String submarineIntpAppName = context.getInterpreterGroupId().toLowerCase().replace("_", "-");
    properties.put("SUBMARINE_JOB_NAME", submarineIntpAppName);

    // setting port range of submarine interpreter container
    String intpPort = properties.getProperty(SubmarineConstants.ZEPPELIN_INTERPRETER_RPC_PORTRANGE,
        String.valueOf(Constants.ZEPPELIN_INTERPRETER_DEFAUlT_PORT));
    String intpPortRange = intpPort + ":" + intpPort;

    // upload configure file to submarine interpreter container
    // keytab file & zeppelin-site.xml & krb5.conf & hadoop-yarn-submarine-X.X.X-SNAPSHOT.jar
    // The submarine configures the mount file into the container through `localization`
    StringBuffer sbLocalization = new StringBuffer();
    // zeppelin-site.xml is uploaded to a specific `${CONTAINER_ZEPPELIN_HOME}` directory in the container
    String zconfFile = zConf.getDocument().getDocumentURI();
    if (zconfFile.startsWith("file:")) {
      zconfFile = zconfFile.replace("file:", "");
    }
    if (!StringUtils.isEmpty(zconfFile)) {
      sbLocalization.append("--localization \"");
      sbLocalization.append(zconfFile + ":" + CONTAINER_ZEPPELIN_HOME + "/zeppelin-site.xml:rw\"");
      sbLocalization.append(" ");
    }

    // ${ZEPPELIN_HOME}/interpreter/submarine is uploaded to a specific `${CONTAINER_ZEPPELIN_HOME}`
    // directory in the container
    String zeppelinHome = getZeppelinHome();
    String intpSubmarinePath = "/interpreter/submarine";
    String zeplIntpSubmarinePath = getPathByHome(zeppelinHome, intpSubmarinePath);
    sbLocalization.append("--localization \"");
    sbLocalization.append(zeplIntpSubmarinePath + ":" + CONTAINER_ZEPPELIN_HOME + intpSubmarinePath + ":rw\"");
    sbLocalization.append(" ");

    // ${ZEPPELIN_HOME}/lib/interpreter is uploaded to a specific `${CONTAINER_ZEPPELIN_HOME}`
    // directory in the container
    String libIntpPath = "/lib/interpreter";
    String zeplLibIntpPath = getPathByHome(zeppelinHome, libIntpPath);
    sbLocalization.append("--localization \"");
    sbLocalization.append(zeplLibIntpPath + ":" + CONTAINER_ZEPPELIN_HOME + libIntpPath + ":rw\"");
    sbLocalization.append(" ");

    // ${ZEPPELIN_HOME}/conf/log4j.properties
    String log4jPath = "/conf/log4j.properties";
    String zeplLog4jPath = getPathByHome(zeppelinHome, log4jPath);
    sbLocalization.append("--localization \"");
    sbLocalization.append(zeplLog4jPath + ":" + CONTAINER_ZEPPELIN_HOME + log4jPath + ":rw\"");
    sbLocalization.append(" ");

    // Keytab file upload container, Keep the same directory as local
    String keytab = properties.getProperty(SubmarineConstants.SUBMARINE_HADOOP_KEYTAB, "");
    keytab = getPathByHome(null, keytab);
    sbLocalization.append("--localization \"");
    sbLocalization.append(keytab + ":" + keytab + ":rw\"").append(" ");

    // krb5.conf file upload container, Keep the same directory as local
    String krb5File = properties.getProperty(SubmarineConstants.SUBMARINE_HADOOP_KRB5_CONF, "");
    krb5File = getPathByHome(null, krb5File);
    sbLocalization.append("--localization \"");
    sbLocalization.append(krb5File + ":" + krb5File + ":rw\"").append(" ");

    // hadoop-yarn-submarine-X.X.X-SNAPSHOT.jar file upload container, Keep the same directory as local
    String submarineJar = properties.getProperty(SubmarineConstants.HADOOP_YARN_SUBMARINE_JAR, "");
    submarineJar = getPathByHome(null, submarineJar);
    sbLocalization.append("--localization \"");
    sbLocalization.append(submarineJar + ":" + submarineJar + ":rw\"").append(" ");

    // hadoop conf directory upload container, Keep the same directory as local
    String hadoopConfDir = properties.getProperty(SubmarineConstants.SUBMARINE_HADOOP_CONF_DIR, "");
    hadoopConfDir = getPathByHome(null, hadoopConfDir);
    sbLocalization.append("--localization \"");
    sbLocalization.append(hadoopConfDir + ":" + hadoopConfDir + ":rw\"").append(" ");

    properties.put("SUBMARINE_LOCALIZATION", sbLocalization.toString());
    LOGGER.info("SUBMARINE_LOCALIZATION=" + sbLocalization.toString());

    // Set the zepplin configuration file path environment variable in `interpreter.sh`
    properties.put("SUBMARINE_ZEPPELIN_CONF_DIR_EVN", "--env ZEPPELIN_CONF_DIR=" + CONTAINER_ZEPPELIN_HOME);

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
          buildEnvFromProperties(context.getInterpreterGroupId(), properties), connectTimeout, name,
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

  protected Map<String, String> buildEnvFromProperties(String intpGroupId, Properties properties) {
    Map<String, String> env = new HashMap<>();
    for (Object key : properties.keySet()) {
      if (RemoteInterpreterUtils.isEnvString((String) key)) {
        env.put((String) key, properties.getProperty((String) key));
      }
    }
    env.put("INTERPRETER_GROUP_ID", intpGroupId);
    return env;
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

    return new ArrayList<Map<String, Object>>() {
    };
  }

  private String getZeppelinHome() {
    String zeppelinHome = "";
    if (System.getenv("ZEPPELIN_HOME") != null) {
      zeppelinHome = System.getenv("ZEPPELIN_HOME");
    }
    if (StringUtils.isEmpty(zeppelinHome)) {
      zeppelinHome = zConf.getString(ZeppelinConfiguration.ConfVars.ZEPPELIN_HOME);
    }
    if (StringUtils.isEmpty(zeppelinHome)) {
      // ${ZEPPELIN_HOME}/plugins/Launcher/SubmarineInterpreterLauncher
      zeppelinHome = getClassPath(SubmarineInterpreterLauncher.class);
      zeppelinHome = zeppelinHome.replace("/plugins/Launcher/SubmarineInterpreterLauncher", "");
    }

    // check zeppelinHome is exist
    File fileZeppelinHome = new File(zeppelinHome);
    if (fileZeppelinHome.exists() && fileZeppelinHome.isDirectory()) {
      return zeppelinHome;
    }

    return "";
  }

  // ${ZEPPELIN_HOME}/interpreter/submarine
  // ${ZEPPELIN_HOME}/lib/interpreter
  private String getPathByHome(String homeDir, String path) throws IOException {
    File file = null;
    if (null == homeDir || StringUtils.isEmpty(homeDir)) {
      file = new File(path);
    } else {
      file = new File(homeDir, path);
    }
    if (file.exists()) {
      return file.getAbsolutePath();
    }

    throw new IOException("Can't find directory in " + homeDir + path + "`!");
  }

  /**
   * -----------------------------------------------------------------------
   * getAppPath needs a class attribute parameter of the Java class used
   * by the current program, which can return the packaged
   * The system directory name where the Java executable (jar, war) is located
   * or the directory where the non-packaged Java program is located
   *
   * @param cls
   * @return The return value is the directory where the Java program
   * where the class is located is running.
   * -------------------------------------------------------------------------
   */
  private String getClassPath(Class cls) {
    // Check if the parameters passed in by the user are empty
    if (cls == null) {
      throw new java.lang.IllegalArgumentException("The parameter cannot be empty!");
    }

    ClassLoader loader = cls.getClassLoader();
    // Get the full name of the class, including the package name
    String clsName = cls.getName() + ".class";
    // Get the package where the incoming parameters are located
    Package pack = cls.getPackage();
    String path = "";
    // If not an anonymous package, convert the package name to a path
    if (pack != null) {
      String packName = pack.getName();
      // Here is a simple decision to determine whether it is a Java base class library,
      // preventing users from passing in the JDK built-in class library.
      if (packName.startsWith("java.") || packName.startsWith("javax.")) {
        throw new java.lang.IllegalArgumentException("Do not transfer system classes!");
      }

      // In the name of the class, remove the part of the package name
      // and get the file name of the class.
      clsName = clsName.substring(packName.length() + 1);
      // Determine whether the package name is a simple package name, and if so,
      // directly convert the package name to a path.
      if (packName.indexOf(".") < 0) {
        path = packName + "/";
      } else {
        // Otherwise, the package name is converted to a path according
        // to the component part of the package name.
        int start = 0, end = 0;
        end = packName.indexOf(".");
        while (end != -1) {
          path = path + packName.substring(start, end) + "/";
          start = end + 1;
          end = packName.indexOf(".", start);
        }
        path = path + packName.substring(start) + "/";
      }
    }
    // Call the classReloader's getResource method, passing in the
    // class file name containing the path information.
    java.net.URL url = loader.getResource(path + clsName);
    // Get path information from the URL object
    String realPath = url.getPath();
    // Remove the protocol name "file:" in the path information.
    int pos = realPath.indexOf("file:");
    if (pos > -1) {
      realPath = realPath.substring(pos + 5);
    }
    // Remove the path information and the part that contains the class file information,
    // and get the path where the class is located.
    pos = realPath.indexOf(path + clsName);
    realPath = realPath.substring(0, pos - 1);
    // If the class file is packaged into a JAR file, etc.,
    // remove the corresponding JAR and other package file names.
    if (realPath.endsWith("!")) {
      realPath = realPath.substring(0, realPath.lastIndexOf("/"));
    }
    try {
      realPath = java.net.URLDecoder.decode(realPath, "utf-8");
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return realPath;
  }
}
