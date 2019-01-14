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


import org.apache.zeppelin.submarine.utils.SubmarineJob;

/**
 * 状态机的上下文环境（根据业务实现自己的状态机流程控制）
 */
public class SubmarineStateMachineContext {
  private boolean active = false;

  private SubmarineJob submarineJob = null;

  public SubmarineStateMachineContext(SubmarineJob submarineJob) {
    this.submarineJob = submarineJob;
  }

  public SubmarineJob getSubmarineJob() {
    return submarineJob;
  }

  public boolean getActiveAndReset() {
    boolean retActive = active;
    active = false;

    return retActive;
  }

  public void setActive(boolean active) {
    this.active = active;
  }
}
