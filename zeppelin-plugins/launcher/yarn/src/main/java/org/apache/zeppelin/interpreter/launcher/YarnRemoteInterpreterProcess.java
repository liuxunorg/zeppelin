package org.apache.zeppelin.interpreter.launcher;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.io.Resources;
import com.hubspot.jinjava.Jinjava;
import org.apache.commons.io.Charsets;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.yarn.submarine.client.cli.Cli;
import org.apache.zeppelin.conf.ZeppelinConfiguration;
import org.apache.zeppelin.interpreter.Constants;
import org.apache.zeppelin.interpreter.remote.RemoteInterpreterProcess;
import org.apache.zeppelin.interpreter.remote.RemoteInterpreterUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.StringTokenizer;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.zeppelin.conf.ZeppelinConfiguration.ConfVars.ZEPPELIN_SERVER_KERBEROS_KEYTAB;
import static org.apache.zeppelin.conf.ZeppelinConfiguration.ConfVars.ZEPPELIN_SERVER_KERBEROS_PRINCIPAL;
import static org.apache.zeppelin.interpreter.launcher.YarnConstants.DOCKER_HADOOP_CONF_DIR;
import static org.apache.zeppelin.interpreter.launcher.YarnConstants.DOCKER_HADOOP_HOME;
import static org.apache.zeppelin.interpreter.launcher.YarnConstants.DOCKER_JAVA_HOME;
import static org.apache.zeppelin.interpreter.launcher.YarnConstants.HADOOP_YARN_SUBMARINE_JAR;
import static org.apache.zeppelin.interpreter.launcher.YarnConstants.SUBMARINE_HADOOP_KEYTAB;
import static org.apache.zeppelin.interpreter.launcher.YarnConstants.SUBMARINE_HADOOP_PRINCIPAL;

public class YarnRemoteInterpreterProcess extends RemoteInterpreterProcess {
  private static final Logger LOGGER = LoggerFactory.getLogger(YarnRemoteInterpreterProcess.class);

  private static final String YARN_SUBMIT_JINJA = "/jinja_templates/yarn-submit.jinja";

  // Zeppelin home path in Docker container
  // Environment variable `SUBMARINE_ZEPPELIN_CONF_DIR_ENV` in /bin/interpreter.sh
  private static final String CONTAINER_ZEPPELIN_HOME = "/zeppelin";

  // Upload local zeppelin library to container, There are several benefits
  // Avoid the difference between the zeppelin version and the local in the container
  // 1). RemoteInterpreterServer::main(String[] args), Different versions of args may be different
  // 2). bin/interpreter.sh Start command args may be different
  // 3). In the debugging phase for easy updating, Upload the local library file to container
  // After the release, Use the zeppelin library file in the container image
  private boolean uploadLocalLibToContainter = true;

  private static boolean isTest = false;

  private Cli submarineCli;

  private static final int YARN_INTERPRETER_SERVICE_PORT
      = Constants.ZEPPELIN_INTERPRETER_DEFAUlT_PORT;

  private AtomicBoolean started = new AtomicBoolean(false);

  private InterpreterLaunchContext intpLaunchContext;
  private Properties properties;
  private ZeppelinConfiguration zconf;

  public YarnRemoteInterpreterProcess(InterpreterLaunchContext context, ZeppelinConfiguration zconf,
                                      int connectTimeout) {
    super(connectTimeout);

    this.intpLaunchContext = context;
    this.properties = context.getProperties();
    this.zconf = zconf;

    String uploadLocalLib = System.getenv("UPLOAD_LOCAL_LIB_TO_CONTAINTER");
    if (null != uploadLocalLib && StringUtils.equals(uploadLocalLib, "false")) {
      uploadLocalLibToContainter = false;
    }
  }

  @Override
  public void start(String userName) throws IOException {
    Map<String, Object> jinjaParams = propertiesToJinjaParams();

    URL urlTemplate = this.getClass().getResource(YARN_SUBMIT_JINJA);
    String template = Resources.toString(urlTemplate, Charsets.UTF_8);
    String yarnSubmit = jinjaRender(template, jinjaParams);
    // If the first line is a newline, delete the newline
    int firstLineIsNewline = yarnSubmit.indexOf("\n");
    if (firstLineIsNewline == 0) {
      yarnSubmit = yarnSubmit.replaceFirst("\n", "");
    }
    yarnSubmit = yarnSubmit.replaceAll("\n", "");
    String[] yarnSubmitArgs = yarnSubmit.split("\\\\");

    for (int n = 0; n < yarnSubmitArgs.length; n++) {
      LOGGER.info("arg[{}]={}", n, yarnSubmitArgs[n]);
    }
    try {
      Cli.main(yarnSubmitArgs);
    } catch (Exception e) {
      LOGGER.error(e.getMessage(), e);
    }
  }

  public String jinjaRender(String template, Map<String, Object> jinjaParams) {
    ClassLoader oldCl = Thread.currentThread().getContextClassLoader();
    try {
      Thread.currentThread().setContextClassLoader(this.getClass().getClassLoader());
      Jinjava jinja = new Jinjava();
      return jinja.render(template, jinjaParams);
    } finally {
      Thread.currentThread().setContextClassLoader(oldCl);
    }
  }

  @Override
  public void stop() {

  }

  @Override
  public String getInterpreterSettingName() {
    return null;
  }

  @Override
  public String getHost() {
    return null;
  }

  @Override
  public int getPort() {
    return 0;
  }

  @Override
  public boolean isRunning() {
    return false;
  }

  @Override
  public void processStarted(int port, String host) {
    LOGGER.info("Interpreter container internal {}:{}", host, port);

    String intpYarnAppName = "";
    //YarnClient.formatYarnAppName(interpreterGroupId);

    // setting port range of submarine interpreter container
    String intpPort = String.valueOf(Constants.ZEPPELIN_INTERPRETER_DEFAUlT_PORT);
    String intpAppHostIp = "";
    String intpAppHostPort = "";
    String intpAppContainerPort = "";
    boolean findExistIntpContainer = false;

    // The submarine interpreter already exists in the connection yarn
    // Or create a submarine interpreter
    // 1. Query the IP and port of the submarine interpreter process through the yarn client
    Map<String, String> exportPorts = null;
    //yarnClient.getAppExportPorts(intpYarnAppName, intpPort);
    if (exportPorts.size() == 0) {
      // It may take a few seconds to query the docker container export port.
      try {
        Thread.sleep(3000);
        //exportPorts = yarnClient.getAppExportPorts(intpYarnAppName, intpPort);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }

    if (exportPorts.containsKey(YarnClient.HOST_IP) && exportPorts.containsKey(YarnClient.HOST_PORT)
        && exportPorts.containsKey(YarnClient.CONTAINER_PORT)) {
      intpAppHostIp = (String) exportPorts.get(YarnClient.HOST_IP);
      intpAppHostPort = (String) exportPorts.get(YarnClient.HOST_PORT);
      intpAppContainerPort = (String) exportPorts.get(YarnClient.CONTAINER_PORT);
      if (StringUtils.equals(intpPort, intpAppContainerPort)) {
        findExistIntpContainer = true;
        LOGGER.info("Detection Submarine interpreter Container " +
                "hostIp:{}, hostPort:{}, containerPort:{}.",
            intpAppHostIp, intpAppHostPort, intpAppContainerPort);
      }
    }

    if (findExistIntpContainer) {
      LOGGER.info("Interpreter container external {}:{}", intpAppHostIp, intpAppHostPort);
      // super.processStarted(Integer.parseInt(intpAppHostPort), intpAppHostIp);
    } else {
      LOGGER.error("Cann't detection Submarine interpreter Container! {}", exportPorts.toString());
    }
  }

  @Override
  public String getErrorMessage() {
    return null;
  }

  protected Map<String, Object> propertiesToJinjaParams()
      throws IOException {
    Map<String, Object> jinjaParams = new HashMap<>();

    // yarn application name match the pattern [a-z][a-z0-9-]*
    String intpYarnAppName = YarnConstants.formatYarnAppName(
        intpLaunchContext.getInterpreterGroupId());
    jinjaParams.put("JOB_NAME", intpYarnAppName);

    // upload configure file to submarine interpreter container
    // keytab file & zeppelin-site.xml & krb5.conf & hadoop-yarn-submarine-X.X.X-SNAPSHOT.jar
    // The submarine configures the mount file into the container through `localization`
    // NOTE: The path to the file uploaded to the container,
    // Can not be repeated, otherwise it will lead to failure.
    HashMap<String, String> uploaded = new HashMap<>();
    List<String> arrLocalization = new ArrayList<>();

    // 1) zeppelin-site.xml is uploaded to `${CONTAINER_ZEPPELIN_HOME}` directory in the container
    if (null != zconf.getDocument()) {
      String zconfFile = zconf.getDocument().getDocumentURI();
      if (zconfFile.startsWith("file:")) {
        zconfFile = zconfFile.replace("file:", "");
      }
      if (!StringUtils.isEmpty(zconfFile)) {
        arrLocalization.add(zconfFile + ":" + CONTAINER_ZEPPELIN_HOME + "/zeppelin-site.xml:rw\"");
      }
    }

    String zeppelinHome = getZeppelinHome();
    if (uploadLocalLibToContainter){
      // 2) ${ZEPPELIN_HOME}/interpreter/submarine is uploaded to `${CONTAINER_ZEPPELIN_HOME}`
      //    directory in the container
      String intpPath = "/interpreter/" + intpLaunchContext.getInterpreterSettingName();
      String zeplIntpSubmarinePath = getPathByHome(zeppelinHome, intpPath);
      arrLocalization.add(zeplIntpSubmarinePath + ":" + CONTAINER_ZEPPELIN_HOME
          + intpPath + ":rw\"");

      // 3) ${ZEPPELIN_HOME}/lib/interpreter is uploaded to `${CONTAINER_ZEPPELIN_HOME}`
      //    directory in the container
      String libIntpPath = "/lib/interpreter";
      String zeplLibIntpPath = getPathByHome(zeppelinHome, libIntpPath);
      arrLocalization.add(zeplLibIntpPath + ":" + CONTAINER_ZEPPELIN_HOME + libIntpPath + ":rw\"");
    }

    // 4) ${ZEPPELIN_HOME}/conf/log4j.properties
    String log4jPath = "/conf/log4j.properties";
    String zeplLog4jPath = getPathByHome(zeppelinHome, log4jPath);
    arrLocalization.add(zeplLog4jPath + ":" + CONTAINER_ZEPPELIN_HOME + log4jPath + ":rw\"");

    // 5) Get the keytab file in each interpreter properties
    // Upload Keytab file to container, Keep the same directory as local host
    // 5.1) shell interpreter properties keytab file
    String intpKeytab = properties.getProperty("zeppelin.shell.keytab.location", "");
    if (StringUtils.isBlank(intpKeytab)) {
      // 5.2) spark interpreter properties keytab file
      intpKeytab = properties.getProperty("spark.yarn.keytab", "");
    }
    if (StringUtils.isBlank(intpKeytab)) {
      // 5.3) submarine interpreter properties keytab file
      intpKeytab = properties.getProperty("submarine.hadoop.keytab", "");
    }
    if (StringUtils.isBlank(intpKeytab)) {
      // 5.4) jdbc interpreter properties keytab file
      intpKeytab = properties.getProperty("zeppelin.jdbc.keytab.location", "");
    }
    if (!StringUtils.isBlank(intpKeytab) && !uploaded.containsKey(intpKeytab)) {
      uploaded.put(intpKeytab, "");
      arrLocalization.add(intpKeytab + ":" + intpKeytab + ":rw\"");
    }

    String zeppelinServerKeytab = zconf.getString(ZEPPELIN_SERVER_KERBEROS_KEYTAB);
    String zeppelinServerPrincipal = zconf.getString(ZEPPELIN_SERVER_KERBEROS_PRINCIPAL);
    if (!StringUtils.isBlank(zeppelinServerKeytab) && !uploaded.containsKey(zeppelinServerKeytab)) {
      uploaded.put(zeppelinServerKeytab, "");
      arrLocalization.add(zeppelinServerKeytab + ":" + zeppelinServerKeytab + ":rw\"");
    }

    // 6) hadoop-yarn-submarine-X.X.X-SNAPSHOT.jar file upload container,
    // Keep the same directory as local
    String submarineJar = System.getenv(HADOOP_YARN_SUBMARINE_JAR);
    if (submarineJar != null && !StringUtils.isEmpty(submarineJar)) {
      submarineJar = getPathByHome(null, submarineJar);
      arrLocalization.add(submarineJar + ":" + submarineJar + ":rw\"");
    }

    if (!isTest) {
      // 7) hadoop conf directory upload container, Keep the same directory as local
      File coreSite = findFileOnClassPath("core-site.xml");
      File hdfsSite = findFileOnClassPath("hdfs-site.xml");
      File yarnSite = findFileOnClassPath("yarn-site.xml");
      if (coreSite == null || hdfsSite == null || yarnSite == null) {
        LOGGER.error("hdfs is being used, however we couldn't locate core-site.xml/"
            + "hdfs-site.xml / yarn-site.xml from classpath, please double check you classpath"
            + "setting and make sure they're included.");
        throw new IOException(
            "Failed to locate core-site.xml / hdfs-site.xml / yarn-site.xml from class path");
      }
      String hadoopConfDir = coreSite.getParent();
      if (!StringUtils.isEmpty(hadoopConfDir)) {
        arrLocalization.add(hadoopConfDir + ":" + hadoopConfDir + ":rw\"");
        // Set the HADOOP_CONF_DIR environment variable in `interpreter.sh`
        jinjaParams.put("DOCKER_HADOOP_CONF_DIR", hadoopConfDir);
      }
    }

    jinjaParams.put("LOCALIZATION_FILES", arrLocalization);
    LOGGER.info("arrLocalization = " + arrLocalization.toString());

    jinjaParams.put(SUBMARINE_HADOOP_KEYTAB, zeppelinServerKeytab);
    jinjaParams.put(SUBMARINE_HADOOP_PRINCIPAL, zeppelinServerPrincipal);

    // Set the zepplin configuration file path environment variable in `interpreter.sh`
    jinjaParams.put("ZEPPELIN_CONF_DIR_ENV", "--env ZEPPELIN_CONF_DIR=" + CONTAINER_ZEPPELIN_HOME);

    String intpContainerRes = zconf.getYarnContainerResource(
        intpLaunchContext.getInterpreterSettingName());
    jinjaParams.put("ZEPPELIN_YARN_CONTAINER_RESOURCE", intpContainerRes);

    String yarnContainerImage = zconf.getYarnContainerImage();
    jinjaParams.put("ZEPPELIN_YARN_CONTAINER_IMAGE", yarnContainerImage);

    for (Object key : properties.keySet()) {
      if (RemoteInterpreterUtils.isEnvString((String) key)) {
        jinjaParams.put((String) key, properties.getProperty((String) key));
      }
    }
    jinjaParams.put("INTERPRETER_GROUP_ID", intpLaunchContext.getInterpreterSettingId());

    // Check environment variables
    String dockerJavaHome = System.getenv(DOCKER_JAVA_HOME);
    String dockerHadoopHome = System.getenv(DOCKER_HADOOP_HOME);
    String dockerHadoopConf = (String) jinjaParams.get(DOCKER_HADOOP_CONF_DIR);
    if (StringUtils.isBlank(dockerJavaHome)) {
      LOGGER.warn("DOCKER_JAVA_HOME environment variables not set!");
    }
    if (StringUtils.isBlank(dockerHadoopHome)) {
      LOGGER.warn("DOCKER_HADOOP_HOME environment variables not set!");
    }
    if (StringUtils.isBlank(dockerHadoopConf)) {
      LOGGER.warn("DOCKER_HADOOP_CONF_DIR environment variables not set!");
    }

    return jinjaParams;
  }

  private String getZeppelinHome() {
    String zeppelinHome = "";
    if (System.getenv("ZEPPELIN_HOME") != null) {
      zeppelinHome = System.getenv("ZEPPELIN_HOME");
    }
    if (StringUtils.isEmpty(zeppelinHome)) {
      zeppelinHome = zconf.getString(ZeppelinConfiguration.ConfVars.ZEPPELIN_HOME);
    }
    if (StringUtils.isEmpty(zeppelinHome)) {
      // ${ZEPPELIN_HOME}/plugins/Launcher/YarnInterpreterLauncher
      zeppelinHome = getClassPath(YarnInterpreterLauncher.class);
      zeppelinHome = zeppelinHome.replace("/plugins/Launcher/YarnInterpreterLauncher", "");
    }

    // check zeppelinHome is exist
    File fileZeppelinHome = new File(zeppelinHome);
    if (fileZeppelinHome.exists() && fileZeppelinHome.isDirectory()) {
      return zeppelinHome;
    }

    return "";
  }

  @VisibleForTesting
  public static void setTest(Boolean test) {
    isTest = test;
  }

  // ${ZEPPELIN_HOME}/interpreter/${interpreter-name}
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

    if (isTest) {
      LOGGER.error("Can't find directory in " + homeDir + path + "`!");
      return "";
    }

    throw new IOException("Can't find directory in " + homeDir + path + "`!");
  }

  private File findFileOnClassPath(final String fileName) {
    final String classpath = System.getProperty("java.class.path");
    final String pathSeparator = System.getProperty("path.separator");
    final StringTokenizer tokenizer = new StringTokenizer(classpath, pathSeparator);

    while (tokenizer.hasMoreTokens()) {
      final String pathElement = tokenizer.nextToken();
      final File directoryOrJar = new File(pathElement);
      final File absoluteDirectoryOrJar = directoryOrJar.getAbsoluteFile();
      if (absoluteDirectoryOrJar.isFile()) {
        final File target = new File(absoluteDirectoryOrJar.getParent(),
            fileName);
        if (target.exists()) {
          return target;
        }
      } else {
        final File target = new File(directoryOrJar, fileName);
        if (target.exists()) {
          return target;
        }
      }
    }

    return null;
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
