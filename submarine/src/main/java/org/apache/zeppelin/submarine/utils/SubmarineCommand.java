package org.apache.zeppelin.submarine.utils;

public class SubmarineCommand {
  private String format;
  private String statementNames;
  private boolean stmtOutputChecked;
  private boolean auditChecked;
  private boolean perStmtChecked;

  public SubmarineCommand(String format, String statementNames,
                          boolean stmtOutputChecked, boolean auditChecked,
                          boolean perStmtChecked) {
    this.format = format;
    this.statementNames = statementNames;
    this.stmtOutputChecked = stmtOutputChecked;
    this.auditChecked = auditChecked;
    this.perStmtChecked = perStmtChecked;
  }
}
