/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.zeppelin.openid;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 *
 */
public enum OpenIdContextService {

  INSTANCE;

  private ThreadLocal<String> user_ip = new ThreadLocal<String>();
  private ThreadLocal<String> user_username = new ThreadLocal<String>();
  private ThreadLocal<String> user_magic = new ThreadLocal<String>();
  private ThreadLocal<String> user_captchaId = new ThreadLocal<String>();
  private ThreadLocal<Long> user_uid = new ThreadLocal<Long>();
  private ThreadLocal<Long> last_time = new ThreadLocal<Long>();

  private final Map<String, Netease.AssociationKeys> KeysMap = new ConcurrentHashMap<>();

  public void clear() {
    user_ip.remove();
    user_username.remove();
    user_magic.remove();
    user_captchaId.remove();
    user_uid.remove();
    last_time.remove();
  }

  public Map<String, Netease.AssociationKeys> getKeysMap() {
    return KeysMap;
  }

  public String getIP() {
    return user_ip.get();
  }

  public void setIP(String ip) {
    user_ip.set(ip);
  }

  public String getUsername() {
    return user_username.get();
  }

  public void setUsername(String username) {
    user_username.set(username);
  }

  public String getMagic() {
    return user_magic.get();
  }

  public void setMagic(String magic) {
    user_magic.set(magic);
  }

  public String getCaptchaId() {
    return user_captchaId.get();
  }

  public void setCaptchaId(String captchaId) {
    user_captchaId.set(captchaId);
  }

  public long getUserId() {
    Long uid = user_uid.get();
    if (uid == null) {
      return -1;
    }
    return uid.longValue();
  }

  public void setUserId(long uid) {
    user_uid.set(uid);
  }

  public long getLastTime() {
    Long last = last_time.get();
    if (last == null) {
      return 0;
    }
    return last.longValue();
  }

  public void SetLastTime(long lastTime) {
    last_time.set(lastTime);
  }
}
