package org.apache.zeppelin.submarine.utils;

import org.apache.commons.exec.*;
import org.apache.commons.lang3.StringUtils;
import org.apache.zeppelin.interpreter.InterpreterOutput;
import org.apache.zeppelin.interpreter.InterpreterResult;
import org.apache.zeppelin.submarine.SubmarineInterpreter;
import org.apache.zeppelin.submarine.SubmarineParagraph;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

public class SubmarineUtils {
  private final boolean isWindows = System.getProperty("os.name").startsWith("Windows");
  private final String shell = isWindows ? "cmd /c" : "bash -c";

  private static final String DIRECTORY_USER_HOME = "shell.working.directory.user.home";
  private static final String TIMEOUT_PROPERTY = "submarine.command.timeout.millisecond";
  private String defaultTimeoutProperty = "60000";

  private String hadoopHome = "";
  private String submarineJar = "";

  Properties property;

  public SubmarineUtils(Properties property) {
    property = property;

    hadoopHome = property.getProperty(SubmarineInterpreter.HADOOP_HOME, "");
    submarineJar = property.getProperty(SubmarineInterpreter.HADOOP_YARN_SUBMARINE_JAR, "");
  }

  public void runJob() {

  }

  // parse submarine command
  // `job run --name distributed-tf-gpu ...`
  // `job show --name distributed-tf-gpu`
  static public String parseCommand(String command) {
    command = command.trim();
    String args[] = command.split(" ");

    if (args.length > 2) {
      return args[1];
    }

    return "";
  }

}
