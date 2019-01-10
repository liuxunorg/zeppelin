package org.apache.zeppelin.submarine.utils;

public enum SubmarineCommand {
  DASHBOARD("DASHBOARD"),
  USAGE("USAGE"),
  JOB_RUN("JOB_RUN"),
  JOB_SHOW("JOB_SHOW"),
  JOB_LIST("JOB_LIST"),
  JOB_CANCEL("JOB_CANCEL"),
  CLEAN_RUNTIME_CACHE("CLEAN_RUNTIME_CACHE"),
  OLD_UI("OLD_UI"),
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
