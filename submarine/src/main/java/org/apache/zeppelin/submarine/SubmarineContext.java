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
