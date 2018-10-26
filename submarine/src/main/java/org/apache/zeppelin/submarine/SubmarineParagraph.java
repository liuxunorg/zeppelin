package org.apache.zeppelin.submarine;

public class SubmarineParagraph {
  private String noteId;
  private String noteName;
  private String paragraphId;
  private String paragraphTitle;
  private String paragraphText;
  private String replName;

  public SubmarineParagraph(String noteId, String paragraphId, String noteName,
                            String paragraphTitle, String paragraphText, String replName) {
    this.noteId = noteId;
    this.noteName = noteName;
    this.paragraphId = paragraphId;
    this.paragraphTitle = paragraphTitle;
    this.paragraphText = paragraphText;
    this.replName = replName;
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
}
