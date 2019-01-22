package org.apache.zeppelin.submarine;

import com.google.common.io.Resources;
import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.LogOutputStream;
import org.apache.commons.exec.PumpStreamHandler;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

public class SubmarineJobTest {
  private static Logger LOGGER = LoggerFactory.getLogger(SubmarineJobTest.class);

  private final boolean isWindows = System.getProperty("os.name").startsWith("Windows");
  private final String shell = isWindows ? "cmd /c" : "bash -c";

  private static final String DEFAULT_EXECUTOR_TEST = "DefaultExecutorTest.sh";

  @Test
  public void defaultExecutorTest() throws IOException {
    DefaultExecutor executor = new DefaultExecutor();
    CommandLine cmdLine = CommandLine.parse(shell);

    URL urlTemplate = Resources.getResource(DEFAULT_EXECUTOR_TEST);

    cmdLine.addArgument(urlTemplate.getFile(), false);

    Map<String, String> env = new HashMap<>();
    env.put("CLASSPATH", "`$HADOOP_HDFS_HOME/bin/hadoop classpath --glob`");
    env.put("EVN", "test");

    StringBuffer sbLogOutput = new StringBuffer();
    executor.setStreamHandler(new PumpStreamHandler(new LogOutputStream() {
      @Override
      protected void processLine(String line, int level) {
        LOGGER.info(line);
      }
    }));

    executor.execute(cmdLine, env);
  }
}
