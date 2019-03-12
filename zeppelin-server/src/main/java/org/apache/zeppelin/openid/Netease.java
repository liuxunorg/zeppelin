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

import org.apache.commons.lang3.StringUtils;

import javax.crypto.Mac;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import javax.net.ssl.HttpsURLConnection;
import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;
import java.io.*;
import java.net.URL;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.Map.Entry;

/**
 *
 */
public class Netease {

  public static final String _uid_c_ = "_u_c_sp_";
  public static final String _uid_ = "_u_sp_";
  public static final String _uid_m_ = "_u_m_sp_";
  public static final String _magic_ = "_)%^&java_sp";

  public static final String openid_server = "https://login.netease.com/openid/";
  public static final String assocHandle = "assoc_handle";
  public static final String macKey = "mac_key";

  public static final String magic(long id, String email, String ip, String captchaId) {
    String hexId = Long.toHexString(id);
    Calendar cal = Calendar.getInstance();
    List<String> strlist = new LinkedList<String>();
    strlist.add(ip);
    strlist.add(Netease._magic_);
    strlist.add(email);
    strlist.add(hexId);
    strlist.add(String.valueOf(cal.get(Calendar.YEAR)));
    strlist.add(String.valueOf(cal.get(Calendar.DAY_OF_YEAR)));
    String magic = MD5MsgDigest.digest(StringUtils.join(strlist, "_"));
    return magic;
  }

  /**
   *
   */
  public static final class AssociationKeys implements Serializable {
    private static final long serialVersionUID = 1153390457086003365L;
    private String assoc_handle = "";
    private String mac_key = "";
    private int expires_in;

    public String getAssoc_handle() {
      return assoc_handle;
    }

    public void setAssoc_handle(String assoc_handle) {
      this.assoc_handle = assoc_handle;
    }

    public String getMac_key() {
      return mac_key;
    }

    public void setMac_key(String mac_key) {
      this.mac_key = mac_key;
    }

    public int getExpires_in() {
      return expires_in;
    }

    public void setExpires_in(int expires_in) {
      this.expires_in = expires_in;
    }
  }

  /**
   *
   */
  public static class CorpUser implements Serializable {
    private static final long serialVersionUID = -2274098816975182178L;
    private String email;
    private String fullname;
    private String nickname;
    private boolean checked = false;
    private String magic = "";

    public String getEmail() {
      return email;
    }

    public void setEmail(String email) {
      this.email = email;
    }

    public String getFullname() {
      return fullname;
    }

    public void setFullname(String fullname) {
      this.fullname = fullname;
    }

    public String getNickname() {
      return nickname;
    }

    public void setNickname(String nickname) {
      this.nickname = nickname;
    }

    public boolean isChecked() {
      return checked;
    }

    public void setChecked(boolean checked) {
      this.checked = checked;
    }

    public String getMagic() {
      return magic;
    }

    public void setMagic(String magic) {
      this.magic = magic;
    }
  }

  /*
   * 将URL的参数以分段的方式进行URL utf8编码，并返回一个字符串
   */
  private static String buildStringUrl(String host, Map<String, String> pairs) {
    StringBuilder url = new StringBuilder(host);
    if (pairs == null || pairs.size() <= 0) {
      return url.toString();
    }
    boolean first = true;
    for (Entry<String, String> pair : pairs.entrySet()) {
      String key = pair.getKey();
      String value = pair.getValue();
      try {
        key = URLEncoder.encode(key, "UTF-8");
        value = URLEncoder.encode(value, "UTF-8");
      } catch (UnsupportedEncodingException e) {
        // ignore exception, use orignal value
      }
      url.append(first ? "?" : "&").append(key).append("=").append(value);
      first = false;
    }
    return url.toString();
  }

  /*
   * OpenId验证的第一步，去获取assoc_handle和mac_key
   */
  public static AssociationKeys association() throws IOException {
    Map<String, String> assoc_data = new HashMap<String, String>();
    assoc_data.put("openid.mode", "associate");
    assoc_data.put("openid.assoc_type", "HMAC-SHA256");
    assoc_data.put("openid.session_type", "no-encryption");

    String strUrl = buildStringUrl(openid_server, assoc_data);
    URL url = new URL(strUrl);
    HttpsURLConnection con = (HttpsURLConnection) url.openConnection();
    /* 接收OpenID返回的值，将assoc_handle, expires_in, mac_key的值存起来 */
    BufferedReader reader = new BufferedReader(
            new InputStreamReader(con.getInputStream(), "UTF-8"));
    AssociationKeys keys = new AssociationKeys();
    try {
      for (String line = ""; (line = reader.readLine()) != null; ) {
        String[] temp_arrays = line.split(":", 2);
        if (temp_arrays.length == 2) {
          if ("assoc_handle".equalsIgnoreCase(temp_arrays[0])) {
            keys.setAssoc_handle(temp_arrays[1]);
          } else if ("expires_in".equalsIgnoreCase(temp_arrays[0])) {
            keys.setExpires_in(Integer.parseInt(temp_arrays[1]));
          } else if ("mac_key".equalsIgnoreCase(temp_arrays[0])) {
            keys.setMac_key(temp_arrays[1]);
          }
        }
      }
      return keys;
    } finally {
      reader.close();
    }
  }

  /*
   * OpenId验证的第二步，去获取请求open id的url连接
   */
  public static String getAuthenticationUrl(String host, String return_to, AssociationKeys keys) {
    Map<String, String> redirect_data = new HashMap<String, String>();
    redirect_data.put("openid.ns", "http://specs.openid.net/auth/2.0");
    redirect_data.put("openid.mode", "checkid_setup");
    // assoc_handle是第一步获取的
    redirect_data.put("openid.assoc_handle", keys.getAssoc_handle());
    // 验证返回的地址
    redirect_data.put("openid.return_to", return_to);
    redirect_data.put("openid.claimed_id", "http://specs.openid.net/auth/2.0/identifier_select");
    redirect_data.put("openid.identity", "http://specs.openid.net/auth/2.0/identifier_select");
    // 服务器地址，要包含return_to的地址
    redirect_data.put("openid.realm", host);
    redirect_data.put("openid.ns.sreg", "http://openid.net/extensions/sreg/1.1");
    redirect_data.put("openid.sreg.required", "nickname,email,fullname");
    redirect_data.put("openid.ns.ax", "http://openid.net/srv/ax/1.0");
    redirect_data.put("openid.ax.mode", "fetch_request");
    redirect_data.put("openid.ax.type.empno", "https://login.netease.com/openid/empno/");
    redirect_data.put("openid.ax.type.dep", "https://login.netease.com/openid/dep/");
    redirect_data.put("openid.ax.required", "empno,dep");
    return buildStringUrl(openid_server, redirect_data);
  }

  /*
   * 将OpenID server返回的URL地址解析，获取参数，参数和值都需要UTF-8解码
   */
  public static Map<String, String> authentication(String responseUrl) {
    Map<String, String> auth_response = new HashMap<String, String>();
    int index = responseUrl.indexOf('?');
    String params = responseUrl.substring(index + 1);
    String[] arrays = params.split("&");
    for (String ar : arrays) {
      String[] subs = ar.split("=", 2);
      String key = subs[0];
      String value = subs[1];
      try {
        key = URLDecoder.decode(subs[0], "UTF-8");
        value = URLDecoder.decode(subs[1], "UTF-8");
      } catch (IOException ex) {
        // ignore
      }
      if (!"captchaId".equals(key)) {
        auth_response.put(key, value);
      }
    }
    return auth_response;
  }

  /*
   * OpenId验证第三步，将server返回的值与assoc中mac_key进行签名计算并验证
   */
  public static CorpUser check_signature(AssociationKeys keys,
                                         Map<String, String> auth_response) throws IOException,
          NoSuchAlgorithmException, InvalidKeyException {
    CorpUser user = new CorpUser();
    user.setEmail(auth_response.get("openid.sreg.email"));
    user.setFullname(auth_response.get("openid.sreg.fullname"));
    user.setNickname(auth_response.get("openid.sreg.nickname"));
    boolean checked = false;

    // openid.mode 返回值不是 id_res, 认证失败!
    if ("id_res".equals(auth_response.get("openid.mode")) == false) {
      // do nothing
    } else if (auth_response.get("openid.assoc_handle").equals(keys.getAssoc_handle()) == false) {
      checked = check_authentication(auth_response, keys);
    } else {
      String[] signed_items = auth_response.get("openid.signed").split(",");
      StringBuilder signed_content = new StringBuilder();
      for (String signed_item : signed_items) {
        signed_content.append(signed_item)
                .append(":").append(auth_response.get("openid." + signed_item))
                .append("\n");
      }
      /* 注意，mac_key也许要先进行base64解码 */
      byte[] decoded64 = (new sun.misc.BASE64Decoder()).decodeBuffer(keys.getMac_key());
      SecretKey signingKey = new SecretKeySpec(decoded64, "HMACSHA256");
      Mac mac = Mac.getInstance("HMACSHA256");
      mac.init(signingKey);
      byte[] digest = mac.doFinal(signed_content.toString().getBytes("UTF-8"));
      /* 计算得到消息摘要之后，需要进行base64编码 */
      String signature = (new sun.misc.BASE64Encoder()).encode(digest);
      checked = signature.equals(auth_response.get("openid.sig"));
    }
    user.setChecked(checked);
    return user;
  }

  /*
   * OpenId验证第四步，该步骤是一个可选步骤，通过server来验证合法性
   */
  public static boolean check_authentication(Map<String,
          String> auth_response, AssociationKeys keys) throws IOException {
    /* 将openid.mode参数的值设置为check_authentication，其他参数和值不变，发回给OpenID server */
    auth_response.put("openid.mode", "check_authentication");
    String strUrl = buildStringUrl(openid_server, auth_response);
    URL url = new URL(strUrl);
    HttpsURLConnection con = (HttpsURLConnection) url.openConnection();
    BufferedReader reader = new BufferedReader(
            new InputStreamReader(con.getInputStream(), "UTF-8"));
    try {
      boolean checked = false;
      for (String str = ""; (str = reader.readLine()) != null; ) {
        String[] temp_arrays = str.split(":", 2);
        if (temp_arrays.length == 2 && "is_valid".equals(temp_arrays[0])) {
          checked = Boolean.valueOf(temp_arrays[1]);
          break;
        }
      }
      return checked;
    } finally {
      reader.close();
    }
  }

  public static String replaceChar(String text, char c, char v) {
    if (StringUtils.isBlank(text)) {
      return text;
    }
    boolean changed = false;
    StringBuilder builder = new StringBuilder();
    for (int i = 0; i < text.length(); i++) {
      char _c = text.charAt(i);
      if (c == _c) {
        changed = true;
        builder.append(v);
      } else {
        builder.append(_c);
      }
    }
    return changed ? builder.toString() : text;
  }


  public static String getCookieValue(HttpServletRequest request, String cookieName) {
    if (StringUtils.isBlank(cookieName)) {
      return "";
    }
    Cookie cookieList[] = request.getCookies();
    if (cookieList == null) {
      return "";
    }
    for (Cookie cookie : cookieList) {
      if (cookieName.equals(cookie.getName())) {
        return cookie.getValue();
      }
    }
    return "";
  }

  public static String getActualIp(HttpServletRequest request) {
    if (request == null) {
      return null;
    }
    String ip = request.getHeader("X-real-ip"); // 联通代理设置的源IP
    if (StringUtils.isNotEmpty(ip)) {
      return ip;
    } else {
      ip = request.getHeader("X-From-IP");
      if (StringUtils.isNotEmpty(ip)) {
        return ip;
      } else {
        return request.getRemoteAddr();
      }
    }
  }
}
