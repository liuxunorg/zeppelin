package org.apache.zeppelin.submarine.utils;

public class SubmarineParagraph {
  private String noteId;
  private String noteName;
  private String paragraphId;
  private String paragraphTitle;
  private String paragraphText;
  private String replName;
  private String cmd;

  public SubmarineParagraph(String noteId, String paragraphId, String noteName,
                            String paragraphTitle, String paragraphText,
                            String replName, String cmd) {
    this.noteId = noteId;
    this.noteName = noteName;
    this.paragraphId = paragraphId;
    this.paragraphTitle = paragraphTitle;
    this.paragraphText = paragraphText;
    this.replName = replName;
    this.cmd = cmd;
  }

  public String getNoteId() {
    return noteId;
  }

  public String getParagraphId() {
    return paragraphId;
  }

  public String getParagraphText() {
    return paragraphText;
  }

  public String getCmd() {
    return cmd;
  }
}
