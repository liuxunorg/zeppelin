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

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.time.DateFormatUtils;
import org.apache.log4j.Logger;

import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.UnsupportedEncodingException;
import java.util.Date;

/**
 *
 */
public class CookieUtils {
  private static final Logger LOGGER = Logger.getLogger(CookieUtils.class);

  public static final int COOKIE_AGE_YEAR = 31536000;

  public static Cookie getCookie(HttpServletRequest request, String cookieName) {
    if (request == null) {
      return null;
    }
    Cookie cookieList[] = request.getCookies();
    if (cookieList == null || cookieName == null)
      return null;
    for (int i = 0; i < cookieList.length; i++) {
      try {
        if (cookieList[i].getName().equals(cookieName)) {
          return cookieList[i];
        }
      } catch (Exception e) {
        LOGGER.error("getCookieValue(HttpServletRequest, String, String)", e);
      }
    }
    return null;
  }

  public static String getCookieValue(HttpServletRequest request, String cookieName) {
    Cookie cookie = getCookie(request, cookieName);
    if (cookie == null) {
      return null;
    }
    return cookie.getValue();
  }

  public static void rmCookie(HttpServletRequest request,
                              HttpServletResponse response,
                              String cookieName) {
    Cookie cookieList[] = request.getCookies();

    if (cookieList == null)
      return;
    for (int i = 0; i < cookieList.length; i++) {
      try {
        if (cookieList[i].getName().equals(cookieName)) {
          cookieList[i].setValue(null);
          cookieList[i].setMaxAge(0);
          cookieList[i].setPath("/");
          response.addCookie(cookieList[i]);
        }
      } catch (Exception e) {
        LOGGER.error("rmCookie(HttpServletRequest, HttpServletResponse, String)", e);
      }
    }
  }

  public static void rmCookie(HttpServletRequest request,
                              HttpServletResponse response,
                              String cookieName,
                              String domain) {
    Cookie cookieList[] = request.getCookies();

    if (cookieList == null)
      return;
    for (int i = 0; i < cookieList.length; i++) {
      try {
        if (cookieList[i].getName().equals(cookieName)) {
          cookieList[i].setValue(null);
          cookieList[i].setMaxAge(0);
          cookieList[i].setPath("/");
          if (StringUtils.isNotBlank(domain)) {
            cookieList[i].setDomain(domain);
          }
          response.addCookie(cookieList[i]);
        }
      } catch (Exception e) {
        LOGGER.error("rmCookie(HttpServletRequest, HttpServletResponse, String)", e);
      }
    }
  }

  public static void setCookie(HttpServletResponse response,
                               String cookieName,
                               String cookieValue,
                               String domain,
                               int cookieMaxage,
                               boolean httpOnly) {
    if (cookieName != null && cookieValue != null) {
      // 判断是否是web请求
      if (httpOnly) {
        // 设置http-only
        String cookieHeader;
        try {
          StringBuilder sb = new StringBuilder(java.net.URLEncoder.encode(cookieName, "UTF-8"))
                  .append("=")
                  .append(cookieValue);
          if (cookieMaxage > 0) {
            // Derived from the format used in RFC 1123

            String dateString = DateFormatUtils
                    .format(new Date(System.currentTimeMillis() + cookieMaxage * 1000L),
                            "EEE, dd MMM yyyy HH:mm:ss zzz");
            sb.append("; Expires=").append(dateString);
          }
          sb.append("; Path=/").append("; HttpOnly");
          if (StringUtils.isNotBlank(domain)) {
            sb.append("; Domain=").append(domain);
          }
          cookieHeader = sb.toString();
          response.addHeader("SET-COOKIE", cookieHeader);
        } catch (UnsupportedEncodingException e) {
          LOGGER.error("setCookie(HttpServletResponse, String, String, int)", e); //$NON-NLS-1$
        }
      } else {
        Cookie theCookie = null;
        try {
          theCookie = new Cookie(java.net.URLEncoder.encode(cookieName, "UTF-8"), cookieValue);
          theCookie.setVersion(1);
          theCookie.setPath("/");
          if (cookieMaxage > 0) {
            theCookie.setMaxAge(cookieMaxage);
          }
          theCookie.setDomain(domain);
          response.addCookie(theCookie);
        } catch (UnsupportedEncodingException e) {
          LOGGER.error("setCookie(HttpServletResponse, String, String, int)", e); //$NON-NLS-1$
        }
      }
    }
  }

}
