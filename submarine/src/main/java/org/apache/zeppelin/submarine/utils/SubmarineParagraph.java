/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.zeppelin.submarine.utils;

public class SubmarineParagraph {
  private String noteId = "";
  private String noteName = "";
  private String paragraphId = "";
  private String paragraphTitle = "";
  private String paragraphScript = "";
  private String replName = "";
  private String cmd = "";

  public SubmarineParagraph() {
  }

  public SubmarineParagraph(String noteId, String paragraphId, String noteName,
                            String paragraphTitle, String paragraphScript,
                            String replName, String cmd) {
    this.noteId = noteId;
    this.noteName = noteName;
    this.paragraphId = paragraphId;
    this.paragraphTitle = paragraphTitle;
    this.paragraphScript = paragraphScript;
    this.replName = replName;
    this.cmd = cmd;
  }

  public String getNoteId() {
    return noteId;
  }

  public String getParagraphId() {
    return paragraphId;
  }

  public String getParagraphScript() {
    return paragraphScript;
  }

  public String getCmd() {
    return cmd;
  }

  public void setParagraphTitle(String paragraphTitle) {
    this.paragraphTitle = paragraphTitle;
  }

  public void setParagraphScript(String paragraphScript) {
    this.paragraphScript = paragraphScript;
  }

  public String getReplName() {
    return replName;
  }

  public void setReplName(String replName) {
    this.replName = replName;
  }

  public String getParagraphTitle() {
    return paragraphTitle;
  }

  public String getNoteName() {
    return noteName;
  }

  public void setNoteName(String noteName) {
    this.noteName = noteName;
  }
}
