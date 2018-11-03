package org.apache.zeppelin.submarine;

import org.apache.zeppelin.submarine.utils.CommandParser;
import org.apache.zeppelin.submarine.utils.SubmarineConstants;
import org.junit.Test;

import java.util.Scanner;

import static org.junit.Assert.assertEquals;

public class CommandParserTest {
  @Test
  public void testParser() {
    CommandParser parser = new CommandParser();

    Scanner sc = new Scanner(SubmarineConstants.TF_INPUT_PATH + "=hi!\n"
        + SubmarineConstants.TF_CHECKPOINT_PATH + " = bar = foobar\t # This is a comment!\n"
        + "# Comment only line\n" // Test handling of comment only lines
        + "\n" // Test handling of empty lines
        + SubmarineConstants.TF_PS_LAUNCH_CMD + " = 1\n"
        + "job run\n"
        + SubmarineConstants.TF_WORKER_LAUNCH_CMD
        + " and long config name containing spaces = phew! it worked!");
    parser.populate(sc);

    assertEquals("hi!", parser.getConfig(SubmarineConstants.TF_INPUT_PATH));
    assertEquals("bar = foobar", parser.getConfig(SubmarineConstants.TF_CHECKPOINT_PATH));
    assertEquals(1, parser.getIntConfig(SubmarineConstants.TF_PS_LAUNCH_CMD));
    assertEquals("job run", parser.getCommand());
    assertEquals("phew! it worked!", parser.getConfig(SubmarineConstants.TF_WORKER_LAUNCH_CMD
        + " AND LONG CONFIG NAME CONTAINING SPACES"));
  }
}
