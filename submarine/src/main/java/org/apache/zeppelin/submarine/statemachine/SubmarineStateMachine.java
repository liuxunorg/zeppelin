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

package org.apache.zeppelin.submarine.statemachine;

import org.apache.zeppelin.submarine.utils.SubmarineCommand;
import org.apache.zeppelin.submarine.utils.SubmarineUI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.squirrelframework.foundation.fsm.impl.AbstractStateMachine;

/**
 * 状态机(也可以使用注解声明式状态机)
 */
public class SubmarineStateMachine extends AbstractStateMachine<SubmarineStateMachine,
    SubmarineStateMachineState, SubmarineStateMachineEvent, SubmarineStateMachineContext> {
  private Logger LOGGER = LoggerFactory.getLogger(SubmarineStateMachine.class);

  public void entryToJobRun(SubmarineStateMachineState from,
                            SubmarineStateMachineState to,
                            SubmarineStateMachineEvent event,
                            SubmarineStateMachineContext context) {
    LOGGER.info("entryToJobRun from:" + from + " to:" + to + " event:" + event);
    SubmarineJob submarineJob = context.getSubmarineJob();
    if (null != submarineJob && null != submarineJob.getSubmarineUI()) {
      SubmarineUI submarineUI = context.getSubmarineJob().getSubmarineUI();
      submarineUI.createSubmarineUI(SubmarineCommand.JOB_RUN);

      if (context.getActiveAndReset()) {
        submarineUI.createLogHeadUI();
        submarineJob.jobRun();
      }
    }
  }

  public void entryToShowJobRun(SubmarineStateMachineState from,
                            SubmarineStateMachineState to,
                            SubmarineStateMachineEvent event,
                            SubmarineStateMachineContext context) {
    LOGGER.info("entryToJobRun from:" + from + " to:" + to + " event:" + event);
    SubmarineJob submarineJob = context.getSubmarineJob();
    if (null != submarineJob && null != submarineJob.getSubmarineUI()) {
      SubmarineUI submarineUI = context.getSubmarineJob().getSubmarineUI();
      submarineUI.createSubmarineUI(SubmarineCommand.JOB_RUN);
    }
  }

  public void entryToJobShow(SubmarineStateMachineState from,
                             SubmarineStateMachineState to,
                             SubmarineStateMachineEvent event,
                             SubmarineStateMachineContext context) {
    LOGGER.info("entryToShowUsage from:" + from + " to:" + to + " event:" + event);
    if (null != context.getSubmarineJob() && null != context.getSubmarineJob().getSubmarineUI()) {
      SubmarineUI submarineUI = context.getSubmarineJob().getSubmarineUI();
      submarineUI.createSubmarineUI(SubmarineCommand.JOB_SHOW);
      if (context.getActiveAndReset()) {
        submarineUI.createLogHeadUI();
      }
    }
  }

  public void entryToInitialization(SubmarineStateMachineState from,
                                    SubmarineStateMachineState to,
                                    SubmarineStateMachineEvent event,
                                    SubmarineStateMachineContext context) {
    LOGGER.info("entryToInitialization" + from + " to:" + to + " event:" + event);
  }

  public void entryToShowUsage(SubmarineStateMachineState from,
                               SubmarineStateMachineState to,
                               SubmarineStateMachineEvent event,
                               SubmarineStateMachineContext context) {
    LOGGER.info("entryToShowUsage from:" + from + " to:" + to + " event:" + event);
    if (null != context && null != context.getSubmarineJob() &&
        null != context.getSubmarineJob().getSubmarineUI()) {
      SubmarineUI submarineUI = context.getSubmarineJob().getSubmarineUI();
      submarineUI.createSubmarineUI(SubmarineCommand.USAGE);
    }
  }

  public void entryToDashboard(SubmarineStateMachineState from,
                               SubmarineStateMachineState to,
                               SubmarineStateMachineEvent event,
                               SubmarineStateMachineContext context) {
    LOGGER.info("entryToDashboard from:" + from + " to:" + to + " event:" + event);
    if (null != context.getSubmarineJob() && null != context.getSubmarineJob().getSubmarineUI()) {
      SubmarineUI submarineUI = context.getSubmarineJob().getSubmarineUI();
      submarineUI.createSubmarineUI(SubmarineCommand.DASHBOARD);
    }
  }

  public void fromAnyToAny(SubmarineStateMachineState from,
                           SubmarineStateMachineState to,
                           SubmarineStateMachineEvent event,
                           SubmarineStateMachineContext context) {
    LOGGER.info("fromAnyToAny from:" + from + " to:" + to + " event:" + event);
  }
}
