package org.apache.zeppelin.submarine;

import org.apache.zeppelin.interpreter.InterpreterContext;
import org.apache.zeppelin.interpreter.InterpreterException;
import org.apache.zeppelin.interpreter.InterpreterResult;
import org.apache.zeppelin.python.PythonInterpreter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class SubmarinePythonInterpreter extends PythonInterpreter {
  private static final Logger LOG = LoggerFactory.getLogger(SubmarinePythonInterpreter.class);

  private SubmarineInterpreter submarineInterpreter = null;
  public SubmarinePythonInterpreter(Properties property) {
    super(property);
  }

  @Override
  public void open() throws InterpreterException {
    this.submarineInterpreter = getInterpreterInTheSameSessionByClassName(SubmarineInterpreter.class);

    super.open();
  }

  @Override
  public InterpreterResult interpret(String st, InterpreterContext context)
      throws InterpreterException {

    return super.interpret(st, context);
  }
}
