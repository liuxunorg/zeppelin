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
package org.apache.zeppelin.rest;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang.StringUtils;
import org.apache.shiro.authc.*;
import org.apache.shiro.subject.Subject;
import org.apache.zeppelin.annotation.ZeppelinApi;
import org.apache.zeppelin.conf.ZeppelinConfiguration;
import org.apache.zeppelin.user.entity.UserKerberos;
import org.apache.zeppelin.notebook.NotebookAuthorization;
import org.apache.zeppelin.openid.CookieUtils;
import org.apache.zeppelin.openid.Netease;
import org.apache.zeppelin.openid.Netease.AssociationKeys;
import org.apache.zeppelin.openid.Netease.CorpUser;
import org.apache.zeppelin.openid.OpenIdContextService;
import org.apache.zeppelin.server.JsonResponse;
import org.apache.zeppelin.service.AuthenticationService;
import org.apache.zeppelin.ticket.TicketContainer;
import org.apache.zeppelin.util.UserKerberosUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * Created for org.apache.zeppelin.rest.message on 17/03/16.
 */
@Path("/openid")
@Produces("application/json")
public class OpenIdRestApi {
  private static final Logger LOG = LoggerFactory.getLogger(OpenIdRestApi.class);

  private AuthenticationService securityService;

  String openidDomain = "";
  String openidRealm = "";

  @Inject
  public OpenIdRestApi(AuthenticationService securityService, ZeppelinConfiguration zconf) {
    this.securityService = securityService;
    openidDomain = zconf.getZeppelinLoginDomain();
    openidRealm = zconf.getZeppelinLoginRealm();
  }

  /**
   * Post Login
   * Returns userName & password
   * for anonymous access, username is always anonymous.
   * After getting this ticket, access through websockets become safe
   *
   * @return 200 response
   */
  @GET
  @Path("login")
  @ZeppelinApi
  public Response openIdLogin(@Context HttpServletRequest req,
                              @Context HttpServletResponse resp) {
    JsonResponse response = null;
    // ticket set to anonymous for anonymous user. Simplify testing.

    Subject currentUser = org.apache.shiro.SecurityUtils.getSubject();
    if (currentUser.isAuthenticated()) {
      currentUser.logout();
    }

    HttpSession session = req.getSession();
    try {
      if (!currentUser.isAuthenticated()) {
        Netease.AssociationKeys keys = Netease.association();
        String captchaId = UUID.randomUUID().toString().replaceAll("-", "");
        String cookie = Netease._uid_c_ + "=" + captchaId + "; Domain="
                + openidDomain + "; Path=/; httponly";
        resp.addHeader("Set-Cookie:", cookie);
        session.setAttribute(captchaId, keys);
        session.setAttribute("captchaId", captchaId);
        OpenIdContextService.INSTANCE.getKeysMap().put(captchaId, keys);
        // 如果没有上一次的url或者上一次是登陆页面则跳转到欢迎页面getKeysMap
        String lastUrl = req.getParameter("last");
        if (StringUtils.isBlank(lastUrl) || lastUrl.toLowerCase().endsWith("/api/openid/login")) {
          lastUrl = URLEncoder.encode(openidRealm);
        }
        String url = Netease.getAuthenticationUrl(
                openidRealm, openidRealm + "/api/openid/cb?captchaId="
                        + captchaId + "&last=" + lastUrl, keys);

        return Response.seeOther(new URI(url)).build();
      }
    } catch (Exception e) {
      response = new JsonResponse(Response.Status.INTERNAL_SERVER_ERROR,
              e.getMessage(), e.getStackTrace().toString());
    }

    if (response == null) {
      response = new JsonResponse(Response.Status.OK, "", "");
    }

    LOG.warn(response.toString());
    return response.build();
  }

  @GET
  @Path("cb")
  @ZeppelinApi
  public Response openIdCallBack(@Context HttpServletRequest req,
                                 @Context HttpServletResponse resp) {
    Response response = null;
    Subject currentUser = org.apache.shiro.SecurityUtils.getSubject();

    if (!currentUser.isAuthenticated()) {
      try {
        CorpUser corpUser = getCorpUser(req, resp);
        if (corpUser == null) {
          throw new AuthenticationException("corp 邮箱验证失败");
        } else {
          String userName = corpUser.getNickname();
          String password = "1";
          UsernamePasswordToken token = new UsernamePasswordToken(userName, password);
          currentUser.login(token);
          Set<String> roles = securityService.getAssociatedRoles();
          String principal = securityService.getPrincipal();
          String ticket;
          if ("anonymous".equals(principal))
            ticket = "anonymous";
          else
            ticket = TicketContainer.instance.getTicket(principal);

          Map<String, String> data = new HashMap<>();
          data.put("principal", principal);
          data.put("roles", roles.toString());
          data.put("ticket", ticket);

          String url = req.getParameter("last");
          response = Response.seeOther(new URI(url)).build();

          //if no exception, that's it, we're done!
          //set roles for user in NotebookAuthorization module
          NotebookAuthorization.getInstance().setRoles(principal, roles);
          req.getSession().setAttribute("user", userName);

          // user kerberos infomation
          UserKerberos userKerberos = UserKerberosUtils.getUserKerberos(userName);
          if (null == userKerberos) {
            UserKerberosUtils.requetUserKerberos(userName, corpUser.getEmail());
          }
        }
      } catch (UnknownAccountException uae) {
        //username wasn't in the system, show them an error message?
        LOG.error("Exception in login: ", uae);
      } catch (IncorrectCredentialsException ice) {
        //password didn't match, try again?
        LOG.error("Exception in login: ", ice);
      } catch (LockedAccountException lae) {
        //account for that username is locked - can't login.  Show them a message?
        LOG.error("Exception in login: ", lae);
      } catch (AuthenticationException ae) {
        //unexpected condition - error?
        LOG.error("Exception in login: ", ae);
      } catch (NoSuchAlgorithmException e) {
        e.printStackTrace();
      } catch (IOException e) {
        e.printStackTrace();
      } catch (InvalidKeyException e) {
        e.printStackTrace();
      } catch (URISyntaxException e) {
        e.printStackTrace();
      }
    }
    if (response == null) {
      response = new JsonResponse(
              Response.Status.FORBIDDEN, "登陆失败", "登陆失败，请联系管理员。").build();
    }

    LOG.warn(response.toString());
    return response;
  }

  public CorpUser getCorpUser(HttpServletRequest req,
                              HttpServletResponse resp) throws NoSuchAlgorithmException,
          InvalidKeyException, IOException {
    OpenIdContextService openIdContextService = OpenIdContextService.INSTANCE;
    HttpSession session = req.getSession(true);
    String captchaId = req.getParameter("captchaId");
    openIdContextService.setCaptchaId(captchaId);

    //AssociationKeys keys = (AssociationKeys) req.getSession().getAttribute(captchaId);
    AssociationKeys keys = openIdContextService.getKeysMap().remove(captchaId);
    String url = req.getQueryString();
    Map<String, String> auth = Netease.authentication(url);
    CorpUser corpUser = Netease.check_signature(keys, auth);
    if (corpUser.isChecked()) {
      String userName = corpUser.getNickname();
      long userId = getUserId(userName);
      session.setAttribute("last_time", System.currentTimeMillis());
      String ip = Netease.getActualIp(req);
      String magic = Netease.magic(userId, corpUser.getEmail(), ip, captchaId);
      CookieUtils.rmCookie(req, resp, Netease._uid_m_);
      CookieUtils.setCookie(resp, Netease._uid_m_, magic, openidDomain, 0, true);
      String uu = Netease.replaceChar(
              new String(Base64.encodeBase64(corpUser.getEmail().getBytes("utf-8"))), '=', '|');
      String uuid = Long.toHexString(userId);
      CookieUtils.rmCookie(req, resp, Netease._uid_);
      CookieUtils.setCookie(resp, Netease._uid_, uuid + "%" + uu, openidDomain, 0, false);
      return corpUser;
    } else {
      return null;
    }
  }

  private Long getUserId(String userName) {
    byte[] bytes = userName.getBytes();
    long result = 0;
    for (int i = 0; i < bytes.length; ++i) {
      int val = bytes[i] & 255;
      result += val;
    }

    return result;
  }

}
