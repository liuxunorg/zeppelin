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
package org.apache.zeppelin.user.entity;

import java.util.Date;

/**
 * User kerberos support Class
 */
public class UserKerberos {
  private int id = -1;

  private String userName;

  private String principal;

  private byte[] keytab;

  private Date dbCreateTime;

  private String email;

  private String clusters;

  public int getId() {
    return id;
  }

  public void setId(int id) {
    this.id = id;
  }

  public String getUserName() {
    return userName;
  }

  public void setUserName(String userName) {
    this.userName = userName;
  }

  public String getPrincipal() {
    return principal;
  }

  public void setPrincipal(String principal) {
    this.principal = principal;
  }

  public byte[] getKeytab() {
    return keytab;
  }

  public void setKeytab(byte[] keytab) {
    this.keytab = keytab;
  }

  public Date getDbCreateTime() {
    return dbCreateTime;
  }

  public void setDbCreateTime(Date dbCreateTime) {
    this.dbCreateTime = dbCreateTime;
  }

  public String getEmail() {
    return email;
  }

  public void setEmail(String email) {
    this.email = email;
  }

  public String getClusters() {
    return clusters;
  }

  public void setClusters(String clusters) {
    this.clusters = clusters;
  }
}
