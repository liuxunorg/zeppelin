package org.apache.zeppelin.submarine;

import java.util.HashMap;
import java.util.Properties;

public class SubmarineContext {
  // noteId:replName -> Properties
  private static HashMap<String, Properties> noteProperties = new HashMap<>();

  public static Properties getProperties(String noteId) {
    Properties properties = null;

    if (!noteProperties.containsKey(noteId)) {
      properties = new Properties();
      noteProperties.put(noteId, properties);
    } else {
      properties = noteProperties.get(noteId);
    }
    return properties;
  }

  public static void setProperties(String noteId, Properties properties) {
    SubmarineContext.noteProperties.put(noteId, properties);
  }

  public static String getPropertie(String noteId, String key) {
    if (noteProperties.containsKey(noteId)) {
      return getProperties(noteId).getProperty(key, "");
    } else {
      return "";
    }
  }
}
