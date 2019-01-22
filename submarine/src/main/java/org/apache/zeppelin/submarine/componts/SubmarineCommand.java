package org.apache.zeppelin.submarine.componts;

public enum SubmarineCommand {
  DASHBOARD("DASHBOARD"),
  USAGE("USAGE"),
  JOB_RUN("JOB_RUN"),
  JOB_SHOW("JOB_SHOW"),
  JOB_LIST("JOB_LIST"),
  JOB_STOP("JOB_STOP"),
  CLEAN_RUNTIME_CACHE("CLEAN_RUNTIME_CACHE"),
  OLD_UI("OLD_UI"),
  TENSORBOARD_RUN("TENSORBOARD_RUN"),
  TENSORBOARD_STOP("TENSORBOARD_STOP"),
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
