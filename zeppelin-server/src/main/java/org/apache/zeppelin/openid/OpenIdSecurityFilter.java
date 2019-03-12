package org.apache.zeppelin.openid;

import org.apache.shiro.subject.Subject;

import javax.servlet.*;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

/**
 *
 */
public class OpenIdSecurityFilter implements Filter {
  @Override
  public void init(FilterConfig filterConfig) throws ServletException {

  }

  @Override
  public void doFilter(ServletRequest servletRequest,
                       ServletResponse servletResponse,
                       FilterChain filterChain) throws IOException, ServletException {
    Subject currentUser = org.apache.shiro.SecurityUtils.getSubject();
    HttpServletRequest httpRequest = (HttpServletRequest) servletRequest;
    if (!currentUser.isAuthenticated() && !httpRequest.getRequestURI().startsWith("/api/openid")) {
      // 检测到未登录或cookies无效
      ((HttpServletResponse) servletResponse).sendRedirect("/api/openid/login");
//      RequestDispatcher dispatcher = servletRequest.getRequestDispatcher("http://www.baidu.com");
//      dispatcher.forward(servletRequest, servletResponse);

//      servletRequest.getRequestDispatcher("https://www.baidu.com/").forward(servletRequest, servletResponse);
    } else {
      if (currentUser.hasRole("dev")) {
        ((HttpServletResponse) servletResponse).sendRedirect(
                "http://zeppelin.bdms.netease.com/api/openid/login");
      } else {
        filterChain.doFilter(servletRequest, servletResponse);
      }
    }
  }

  @Override
  public void destroy() {

  }
}
