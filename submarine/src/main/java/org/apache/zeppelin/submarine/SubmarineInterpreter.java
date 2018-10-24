/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.zeppelin.submarine;

import org.apache.zeppelin.interpreter.KerberosInterpreter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * SubmarineInterpreter of Hadoop Submarine implementation.
 * Support for Hadoop Submarine cli. All the commands documented here
 * https://github.com/apache/hadoop/blob/trunk/hadoop-yarn-project/hadoop-yarn/
 * hadoop-yarn-applications/hadoop-yarn-submarine/src/site/markdown/QuickStart.md is supported.
 */
public abstract class SubmarineInterpreter extends KerberosInterpreter {
  private Logger LOGGER = LoggerFactory.getLogger(SubmarineInterpreter.class);

  public static final String HADOOP_YARN_SUBMARINE_BIN = "hadoop.yarn.submarine.bin";
  public static final String SUBMARINE_YARN_QUEUE = "submarine.yarn.queue";

  public SubmarineInterpreter(Properties property) {
    super(property);
  }

  @Override
  public FormType getFormType() {
    return FormType.SIMPLE;
  }
}
