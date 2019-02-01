package org.apache.zeppelin.submarine;

import org.apache.zeppelin.interpreter.InterpreterException;
import org.apache.zeppelin.python.IPythonInterpreter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class IPySubmarineInterpreter extends IPythonInterpreter {

  private static final Logger LOGGER = LoggerFactory.getLogger(IPySubmarineInterpreter.class);

  private PySubmarineInterpreter pySubmarineInterpreter;

  public IPySubmarineInterpreter(Properties properties) {
    super(properties);
  }

  @Override
  public void open() throws InterpreterException {
    PySubmarineInterpreter pySparkInterpreter =
        getInterpreterInTheSameSessionByClassName(PySubmarineInterpreter.class, false);

    pySubmarineInterpreter
        = getInterpreterInTheSameSessionByClassName(PySubmarineInterpreter.class);

    super.open();
  }
}
