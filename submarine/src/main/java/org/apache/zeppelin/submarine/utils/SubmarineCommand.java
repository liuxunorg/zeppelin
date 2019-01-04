package org.apache.zeppelin.submarine.utils;

public enum SubmarineCommand {
  DASHBOARD("DASHBOARD"),
  USAGE("USAGE"),
  JOB_RUN("JOB RUN"),
  JOB_SHOW("JOB SHOW"),
  JOB_LIST("JOB LIST"),
  JOB_CANCEL("JOB CANCEL"),
  EMPTY(""),
  OLD_UI("OLD UI"),
  UNKNOWN("Unknown");

  private String command;

  SubmarineCommand(String command){
    this.command = command;
  }

  public String getCommand(){
    return command;
  }

  public static SubmarineCommand fromCommand(String command) {
    for (SubmarineCommand type : SubmarineCommand.values()) {
      if (type.getCommand().equals(command)) {
        return type;
      }
    }

    return UNKNOWN;
  }
}
