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
package org.apache.zeppelin.user;

import org.apache.ibatis.session.SqlSession;
import org.apache.log4j.Logger;
import org.apache.zeppelin.user.entity.UserKerberos;
import org.apache.zeppelin.util.MyBatisUtils;

import java.util.HashMap;
import java.util.Map;

/**
 * ServiceMapper
 */
public class ServiceMapper {
  private static final Logger logger = Logger.getLogger(ServiceMapper.class.getName());

  public UserKerberos getUserKerberos(String userName) throws Exception {
    logger.info("getUserKerberos(" + userName + ") <<<");

    UserKerberos userKerberos = null;
    SqlSession sqlSession = MyBatisUtils.getSqlSessionFactory().openSession();

    try {
      Map<String, Object> params = new HashMap<>();
      params.put("user_name", userName);
      String statement = "org.apache.zeppelin.user.ServiceMapper.getUserKerberos";
      userKerberos = sqlSession.selectOne(statement, params);
    } catch (Exception e) {
      e.printStackTrace();
      logger.error(e.getMessage());
      throw new Exception("getUserKerberos", e);
    } finally {
      sqlSession.close();
    }

    logger.info("getUserKerberos() <<<");
    return userKerberos;
  }

  public boolean updateUserKerberos(UserKerberos userKerberos) throws Exception {
    logger.info("updateUserKerberos(" + userKerberos.getUserName() + ") <<<");

    SqlSession sqlSession = MyBatisUtils.getSqlSessionFactory().openSession();

    try {
      Map<String, Object> params = new HashMap<>();
      params.put("item", userKerberos);
      if (-1 == userKerberos.getId()) {
        String statement = "org.apache.zeppelin.user.ServiceMapper.insertUserKerberos";
        sqlSession.insert(statement, params);
      } else {
        String statement = "org.apache.zeppelin.user.ServiceMapper.updateUserKerberos";
        int num = sqlSession.update(statement, params);
      }
      sqlSession.commit();
    } catch (Exception e) {
      e.printStackTrace();
      logger.error(e.getMessage());
      throw new Exception("getUserKerberos", e);
    } finally {
      sqlSession.close();
    }

    logger.info("updateUserKerberos() <<<");
    return true;
  }
}
