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

package org.apache.zeppelin.terminal.helper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;

public class ThreadHelper {
  private static final Logger LOGGER = LoggerFactory.getLogger(ThreadHelper.class);
  private static final Semaphore uiSemaphore = new Semaphore(1);
  private static final ExecutorService singleExecutorService = Executors.newSingleThreadExecutor();


  private static void releaseUiSemaphor() {
    singleExecutorService.submit(() -> {
      uiSemaphore.release();
    });
  }

  public static void start(Runnable runnable) {
    Thread thread = new Thread(runnable);
    thread.start();
  }

  public static void sleep(int millis) {
    try {
      Thread.sleep(millis);
    } catch (InterruptedException e) {
      LOGGER.error(e.getMessage(), e);
    }
  }
}
