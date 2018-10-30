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
