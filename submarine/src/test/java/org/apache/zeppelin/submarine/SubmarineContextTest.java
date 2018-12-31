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

package org.apache.zeppelin.submarine;

import org.apache.zeppelin.conf.ZeppelinConfiguration;
import org.apache.zeppelin.submarine.utils.SubmarineParagraph;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Properties;

public class SubmarineContextTest {
  private static Logger LOGGER = LoggerFactory.getLogger(SubmarineContextTest.class);

  static SubmarineContext submarineContext = null;
  static ZeppelinConfiguration zConf = ZeppelinConfiguration.create();

  static ArrayList<File> noteFiles = new ArrayList();

  @BeforeClass
  public static void initEnv() {
    Properties properties = new Properties();
    submarineContext = SubmarineContext.getInstance(properties);

    File directory = new File("");
    try {
      String courseFile = directory.getCanonicalPath();
      String notebookPath = new File(courseFile).getParent() + File.separator + "notebook";
      zConf.setNotebookDir(notebookPath);

      getFileList(notebookPath, noteFiles);
      LOGGER.info("noteFiles.size = {}", noteFiles.size());
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public static void getFileList(String strPath, ArrayList<File> filelist) {
    File dir = new File(strPath);
    File[] files = dir.listFiles();
    if (files != null) {
      for (int i = 0; i < files.length; i++) {
        String fileName = files[i].getName();
        if (files[i].isDirectory()) {
          getFileList(files[i].getAbsolutePath(), filelist);
        } else if (fileName.endsWith(".zpln") || fileName.endsWith(".json")){
          String strFileName = files[i].getAbsolutePath();
          LOGGER.info("Found note : " + strFileName);
          filelist.add(files[i]);
        }
      }
    }
  }

  @Test
  public void getNoteParagraphTest() {
    String notebookDir = zConf.getNotebookDir();

    for (int i = 0; i < noteFiles.size(); i++) {
      String noteFileName = noteFiles.get(i).getAbsolutePath().replace(notebookDir, "");
      ArrayList<SubmarineParagraph> paragraphs
          = submarineContext.getNoteParagraphs(noteFileName);
      LOGGER.info("paragraphs.size = {}", paragraphs.size());
    }
  }

  @Test
  public void splitParagraphToFilesTest() {
    Properties properties = new Properties();

    String notebookDir = zConf.getNotebookDir();

    for (int i = 0; i < noteFiles.size(); i++) {
      String noteFileName = noteFiles.get(i).getAbsolutePath().replace(notebookDir, "");

      String outMsg = submarineContext.saveParagraphToFiles(noteFileName,
          "/Users/liuxun/Downloads/saveNoteParagraphTest", "", properties);
      LOGGER.info(outMsg);
    }
  }
}
