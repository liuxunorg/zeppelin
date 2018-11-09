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

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonIOException;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonSyntaxException;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.zeppelin.conf.ZeppelinConfiguration;
import org.apache.zeppelin.submarine.utils.HDFSUtils;
import org.apache.zeppelin.submarine.utils.SubmarineConstants;
import org.apache.zeppelin.submarine.utils.SubmarineParagraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SubmarineContext {
  private Logger LOGGER = LoggerFactory.getLogger(SubmarineContext.class);

  private static SubmarineContext instance = null;

  private static ZeppelinConfiguration zConf = ZeppelinConfiguration.create();

  // noteId:replName -> Properties
  private HashMap<String, Properties> noteProperties = new HashMap<>();
  private HDFSUtils hdfsUtils;

  public static final String DEFAULT_STORAGE
      = "org.apache.zeppelin.notebook.repo.GitNotebookRepo";
  public static final String HDFS_STORAGE
      = "org.apache.zeppelin.notebook.repo.FileSystemNotebookRepo";
  private String noteStorageClassName = "";

  private static Pattern REPL_PATTERN =
      Pattern.compile("(\\s*)%([\\w\\.]+)(\\(.*?\\))?.*", Pattern.DOTALL);

  public static SubmarineContext getInstance(Properties properties) {
    synchronized (SubmarineContext.class) {
      if (instance == null) {
        instance = new SubmarineContext("/", properties);
      }
      return instance;
    }
  }

  public SubmarineContext(String path, Properties properties) {
    hdfsUtils = new HDFSUtils(path, properties);
  }

  public HDFSUtils getHDFSUtils() {
    return hdfsUtils;
  }

  public void saveParagraphToFiles(String noteId, String noteName, String pythonWorkDir) {
    Properties properties = getProperties(noteId);

    // zeppelin 0.9 version note name format
    String noteFileName = noteName + "_" + noteId + ".zpln";

    String hdfsUploadPath = properties.getProperty(
        SubmarineConstants.SUBMARINE_ALGORITHM_HDFS_PATH, "");
    splitParagraphToFiles(noteFileName, pythonWorkDir, hdfsUploadPath);
  }

  private void splitParagraphToFiles(String noteFileName, String dirName, String hdfsUploadPath) {
    ArrayList<SubmarineParagraph> paragraphs = getNoteParagraphs(noteFileName);

    HashMap<String, StringBuffer> mapParagraph = new HashMap<>();
    for (SubmarineParagraph paragraph : paragraphs) {
      String paragraphTitle = paragraph.getParagraphTitle();
      if (org.apache.commons.lang.StringUtils.isEmpty(paragraphTitle)) {
        // TODO(liuxun): output warn
        LOGGER.warn("paragraph title is null");
        continue;
      }
      if (!mapParagraph.containsKey(paragraphTitle)) {
        StringBuffer mergeScript = new StringBuffer();
        mapParagraph.put(paragraphTitle, mergeScript);
      }
      StringBuffer mergeScript = mapParagraph.get(paragraphTitle);
      mergeScript.append(paragraph.getParagraphScript() + "\n\n");
    }

    for (Map.Entry<String, StringBuffer> entry : mapParagraph.entrySet()) {
      try {
        String fileName = entry.getKey();
        String fileContext = entry.getValue().toString();
        String paragraphFile = dirName + "/" + fileName;

        // save to local file
        if (!StringUtils.isEmpty(dirName)) {
          File fileParagraph = new File(paragraphFile);
          if (!fileParagraph.exists()) {
            File dir = new File(fileParagraph.getParent());
            dir.mkdirs();
            fileParagraph.createNewFile();
          }
          FileWriter writer = new FileWriter(paragraphFile);
          writer.write(fileContext);
          writer.close();
        }

        // save to hdfs
        if (!StringUtils.isEmpty(hdfsUploadPath)) {
          Path hdfsPath = new Path(hdfsUploadPath);
          if (!hdfsUtils.exists(hdfsPath)) {
            hdfsUtils.tryMkDir(hdfsPath);
          }
          String fileDir = hdfsUploadPath + File.separator + fileName;
          // upload algorithm file
          LOGGER.info("Commit algorithm to HDFS: {}", fileDir);
          Path filePath = new Path(fileDir);
          hdfsUtils.writeFile(fileContext, filePath);
        }
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }

  public ArrayList<SubmarineParagraph> getNoteParagraphs(String noteFileName) {
    if (StringUtils.isEmpty(noteStorageClassName)) {
      String allStorageClassNames = zConf.getNotebookStorageClass().trim();
      if (allStorageClassNames.isEmpty()) {
        allStorageClassNames = DEFAULT_STORAGE;
        LOGGER.warn("Empty ZEPPELIN_NOTEBOOK_STORAGE conf parameter, using default {}",
            DEFAULT_STORAGE);
      }
      String[] storageClassNames = allStorageClassNames.split(",");
      noteStorageClassName = DEFAULT_STORAGE;
      for (int i = 0; i < storageClassNames.length; i++) {
        if (storageClassNames[i].equalsIgnoreCase(HDFS_STORAGE)) {
          noteStorageClassName = HDFS_STORAGE;
          break;
        }
      }
    }

    String noteAbsolutePath = zConf.getNotebookDir();
    String noteContext = "";
    if (!noteAbsolutePath.endsWith(File.separator)) {
      noteAbsolutePath = noteAbsolutePath + File.separator;
    }
    noteAbsolutePath = noteAbsolutePath + noteFileName;

    try {
      if (noteStorageClassName.equalsIgnoreCase(DEFAULT_STORAGE)) {
        File file = new File(noteAbsolutePath);
        if (file.isFile() && file.exists()) {
          InputStreamReader read = new InputStreamReader(new FileInputStream(file));
          BufferedReader bufferedReader = new BufferedReader(read);
          String line = "";
          while ((line = bufferedReader.readLine()) != null) {
            noteContext += line;
          }
        }
      } else if (noteStorageClassName.equalsIgnoreCase(HDFS_STORAGE)) {
        Path notePath = new Path(noteAbsolutePath);
        if (hdfsUtils.exists(notePath)) {
          noteContext = hdfsUtils.readFile(notePath);
        }
      } else {
        LOGGER.error("SubmarineContext.getNote() only support {} or {}",
            DEFAULT_STORAGE, HDFS_STORAGE);
      }
    } catch (Exception e) {
      e.printStackTrace();
    }

    return parseNote(noteContext);
  }

  private ArrayList<SubmarineParagraph> parseNote(String noteContext) {
    ArrayList<SubmarineParagraph> paragraphs = new ArrayList<>();
    try {
      JsonParser jsonParser = new JsonParser();
      JsonObject jsonObject = (JsonObject) jsonParser.parse(noteContext);
      JsonArray jsonParagraphs = jsonObject.get("paragraphs").getAsJsonArray();
      for (int i = 0; i < jsonParagraphs.size(); i++) {
        JsonObject jsonParagraph = jsonParagraphs.get(i).getAsJsonObject();

        JsonElement jsonElement = jsonParagraph.get("title");
        String title = (jsonElement == null) ? "" : jsonElement.getAsString();
        jsonElement = jsonParagraph.get("text");
        String text = (jsonElement == null) ? "" : jsonElement.getAsString();

        SubmarineParagraph paragraph = parseScript(text);
        paragraph.setParagraphTitle(title);

        paragraphs.add(paragraph);
      }
    } catch (JsonIOException e) {
      e.printStackTrace();
    } catch (JsonSyntaxException e) {
      e.printStackTrace();
    }

    return paragraphs;
  }

  private SubmarineParagraph parseScript(String script) {
    Map<String, String> localProperties = new HashMap<>();
    String replName = "", codeText = "";
    // parse text to get interpreter component
    if (script != null) {
      // clean localProperties, otherwise previous localProperties will be used for the next run
      localProperties.clear();
      Matcher matcher = REPL_PATTERN.matcher(script);
      if (matcher.matches()) {
        String headingSpace = matcher.group(1);
        replName = matcher.group(2);
        if (matcher.groupCount() == 3 && matcher.group(3) != null) {
          String localPropertiesText = matcher.group(3);
          codeText = script.substring(headingSpace.length() + replName.length() +
              localPropertiesText.length() + 1).trim();
        } else {
          codeText = script.substring(headingSpace.length() + replName.length() + 1).trim();
        }
      } else {
        replName = "";
        codeText = script.trim();
      }
    }

    SubmarineParagraph paragraph = new SubmarineParagraph();
    paragraph.setReplName(replName);
    paragraph.setParagraphScript(codeText);

    return paragraph;
  }

  public Properties getProperties(String noteId) {
    Properties properties = null;

    if (!noteProperties.containsKey(noteId)) {
      properties = new Properties();
      noteProperties.put(noteId, properties);
    } else {
      properties = noteProperties.get(noteId);
    }
    return properties;
  }

  public void setProperties(String noteId, Properties properties) {
    noteProperties.put(noteId, properties);
  }

  public boolean setProperties(String noteId, String key, String value) {
    if (noteProperties.containsKey(noteId)) {
      getProperties(noteId).setProperty(key, value);
    } else {
      return false;
    }

    return true;
  }

  public String getPropertie(String noteId, String key) {
    if (noteProperties.containsKey(noteId)) {
      return getProperties(noteId).getProperty(key, "");
    } else {
      return "";
    }
  }
}
