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

package org.apache.zeppelin.interpreter.launcher;

import org.apache.zeppelin.conf.ZeppelinConfiguration;
import org.apache.zeppelin.interpreter.InterpreterOption;
import org.apache.zeppelin.interpreter.InterpreterRunner;
import org.apache.zeppelin.interpreter.recovery.RecoveryStorage;
import org.apache.zeppelin.interpreter.remote.RemoteInterpreterRunningProcess;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;

/**
 * Yarn specific launcher.
 */
public class YarnInterpreterLauncher extends StandardInterpreterLauncher {
  private static final Logger LOGGER = LoggerFactory.getLogger(YarnInterpreterLauncher.class);

  private ZeppelinConfiguration zconf;

  public YarnInterpreterLauncher(ZeppelinConfiguration zConf, RecoveryStorage recoveryStorage) {
    super(zConf, recoveryStorage);

    this.zconf = zConf;
  }

  @Override
  public InterpreterClient launch(InterpreterLaunchContext context) throws IOException {
    LOGGER.info("YarnInterpreterLauncher: " + context.getInterpreterSettingGroup());

    // Because need to modify the properties, make a clone
    this.properties = (Properties) context.getProperties().clone();

    InterpreterOption option = context.getOption();
    InterpreterRunner runner = context.getRunner();
    String groupName = context.getInterpreterSettingGroup();
    String name = context.getInterpreterSettingName();
    int connectTimeout = getConnectTimeout();
    if (connectTimeout < 200000) {
      // Because yarn need to download docker image,
      // So need to increase the timeout setting.
      connectTimeout = 200000;
    }

    if (option.isExistingProcess()) {
      LOGGER.info("Connect an existing remote interpreter process.");
      return new RemoteInterpreterRunningProcess(
          context.getInterpreterSettingName(),
          connectTimeout,
          option.getHost(),
          option.getPort());
    } else {
      return new YarnRemoteInterpreterProcess(context, zconf, connectTimeout);
    }
  }
}
