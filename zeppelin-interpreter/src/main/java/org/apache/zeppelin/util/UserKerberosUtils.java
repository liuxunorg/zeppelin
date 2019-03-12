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
package org.apache.zeppelin.util;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.http.HttpEntity;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.ByteArrayBuffer;
import org.apache.http.util.EntityUtils;
import org.apache.log4j.Logger;
import org.apache.zeppelin.conf.ZeppelinConfiguration;
import org.apache.zeppelin.user.ServiceMapper;
import org.apache.zeppelin.user.entity.UserKerberos;

import java.io.*;
import java.util.Arrays;

/**
 * UserKerberosUtils
 */
public class UserKerberosUtils {
  private static final Logger logger = Logger.getLogger(UserKerberosUtils.class.getName());

  public static UserKerberos getUserKerberos(String userName) {
    UserKerberos userKerberos = null;

    ServiceMapper serviceMapper = new ServiceMapper();
    try {
      userKerberos = serviceMapper.getUserKerberos(userName);
      if (null != userKerberos) {
        ZeppelinConfiguration conf = ZeppelinConfiguration.create();
        String keytabStoragePath = conf.getZeppelinUserKeytabStoragePath();
        String keytabPath = keytabStoragePath + File.separator + userName + ".keytab";
        File keytabFile = new File(keytabPath);
        if (!keytabFile.exists()) {
          // recreate keytab file
          saveKeytabFile(userName, userKerberos.getKeytab());
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
    }

    return userKerberos;
  }

  public static UserKerberos requetUserKerberos(String userName, String userEmail)
      throws IOException {
    UserKerberos userKerberos = new UserKerberos();
    ZeppelinConfiguration conf = ZeppelinConfiguration.create();
    String kerberosRestUrl = conf.getZeppelinUserKerberosRestUrl();
    kerberosRestUrl = kerberosRestUrl + "/" + userEmail + "/getkey";
    logger.info("kerberosRestUrl = " + kerberosRestUrl);

    CloseableHttpClient httpclient = null;
    CloseableHttpResponse response = null;

    try {
      httpclient = HttpClients.createDefault();
      HttpGet httpget = new HttpGet(kerberosRestUrl);
      response = httpclient.execute(httpget);
      RequestConfig localConfig = RequestConfig.custom()
          .setConnectTimeout(3000)
          .setSocketTimeout(3000)
          .build();
      httpget.setConfig(localConfig);
      HttpEntity entity = response.getEntity();
      if (entity.getContentType().getValue().equals("application/json")){
        logger.error(String.format("req url failed, url: %s, content: %s", kerberosRestUrl,
            EntityUtils.toString(entity, "utf-8")));
        return null;
      }

      if (entity != null) {
        final InputStream instream = entity.getContent();
        if (instream == null) {
          return null;
        }
        try {
          int contentLength = (int) entity.getContentLength();
          if (contentLength < 0) {
            contentLength = 4096;
          }
          final ByteArrayBuffer buffer = new ByteArrayBuffer(contentLength);
          final byte[] tmp = new byte[4096];
          int read;
          while ((read = instream.read(tmp)) != -1) {
            buffer.append(tmp, 0, read);
          }
          byte[] bytes = buffer.toByteArray();
          String principal = UserKerberosUtils.parsePrincipal(bytes);
          byte[] keytabBytes = UserKerberosUtils.parseKeytabBytes(bytes);

          // write keytab file
          saveKeytabFile(userName, keytabBytes);

          userKerberos.setUserName(userName);
          userKerberos.setPrincipal(principal);
          userKerberos.setKeytab(keytabBytes);
          userKerberos.setEmail(userEmail);
          ServiceMapper serviceMapper = new ServiceMapper();
          serviceMapper.updateUserKerberos(userKerberos);
        } catch (Exception e) {
          logger.error(e.getMessage());
        } finally {
          instream.close();
        }
      }
    } catch (Exception e) {
      logger.error(e.getMessage());
    } finally {
      httpclient.close();
      response.close();
    }

    return userKerberos;
  }

  private static String saveKeytabFile(String userName, byte[] keytabBytes) {
    ZeppelinConfiguration conf = ZeppelinConfiguration.create();
    String keytabStoragePath = conf.getZeppelinUserKeytabStoragePath();
    File fPath = new File(keytabStoragePath);
    if (!fPath.exists()) {
      fPath.mkdirs();
    }

    String keytabPath = keytabStoragePath + File.separator + userName + ".keytab";
    try {
      File file = new File(keytabPath);
      FileOutputStream fos = new FileOutputStream(file);
      fos.write(keytabBytes);
      fos.close();
    } catch (FileNotFoundException e) {
      e.printStackTrace();
      logger.error(e.getMessage());
    } catch (IOException e) {
      e.printStackTrace();
      logger.error(e.getMessage());
    }

    return keytabPath;
  }

  // Parse the byte array of the principal
  private static String parsePrincipal(byte[] bytes) {
    String principal;
    if (bytes.length > 0) {
      int len = (int) bytes[0];
      if (len <= 0)
        return null;
      if (bytes.length < len + 1)
        return null;

      byte[] pBytes = new byte[len];
      System.arraycopy(bytes, 1, pBytes, 0, len);

      try {
        principal = new String(pBytes, "utf-8");
      } catch (UnsupportedEncodingException e) {
        logger.error("parse principal error", e);
        return null;
      }
      return principal;
    }
    return "";
  }

  // Parse the byte array of the key table file
  private static byte[] parseKeytabBytes(byte[] bytes) {
    if (bytes.length > 0) {
      int len = (int) bytes[0];
      if (len <= 0)
        return null;
      if (bytes.length <= len + 1)
        return null;
      return Arrays.copyOfRange(bytes, len + 1, bytes.length);
    }
    return null;
  }
}
