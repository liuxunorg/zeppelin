package org.apache.zeppelin.submarine;

import org.apache.zeppelin.submarine.statemachine.SubmarineStateMachine;
import org.apache.zeppelin.submarine.statemachine.SubmarineStateMachineContext;
import org.apache.zeppelin.submarine.statemachine.SubmarineStateMachineEvent;
import org.apache.zeppelin.submarine.statemachine.SubmarineStateMachineState;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.squirrelframework.foundation.fsm.StateMachineBuilder;
import org.squirrelframework.foundation.fsm.StateMachineBuilderFactory;
import org.squirrelframework.foundation.fsm.StateMachineConfiguration;

import static org.apache.zeppelin.submarine.statemachine.SubmarineStateMachineEvent.ToDashboard;
import static org.apache.zeppelin.submarine.statemachine.SubmarineStateMachineEvent.ToJobRun;
import static org.apache.zeppelin.submarine.statemachine.SubmarineStateMachineEvent.ToJobShow;
import static org.apache.zeppelin.submarine.statemachine.SubmarineStateMachineEvent.ToShowUsage;
import static org.apache.zeppelin.submarine.statemachine.SubmarineStateMachineState.DASHBOARD;
import static org.apache.zeppelin.submarine.statemachine.SubmarineStateMachineState.INITIALIZATION;
import static org.apache.zeppelin.submarine.statemachine.SubmarineStateMachineState.JOB_RUN;
import static org.apache.zeppelin.submarine.statemachine.SubmarineStateMachineState.JOB_SHOW;
import static org.apache.zeppelin.submarine.statemachine.SubmarineStateMachineState.SHOW_USAGE;

public class StateMachineTest {
  private static Logger LOGGER = LoggerFactory.getLogger(StateMachineTest.class);

  static SubmarineStateMachine submarineStateMachine = null;

  @BeforeClass
  public static void initEnv() {
    try {
      StateMachineBuilder<SubmarineStateMachine, SubmarineStateMachineState,
          SubmarineStateMachineEvent, SubmarineStateMachineContext> builder =
          StateMachineBuilderFactory.create(SubmarineStateMachine.class,
              SubmarineStateMachineState.class, SubmarineStateMachineEvent.class,
              SubmarineStateMachineContext.class);

      builder.onEntry(INITIALIZATION).callMethod("entryToInitialization");

      builder.transit().from(INITIALIZATION).to(SHOW_USAGE)
          .on(ToShowUsage).callMethod("fromAnyToAny");
      builder.onEntry(SHOW_USAGE).callMethod("entryToShowUsage");

      // Any -> JobRun
      builder.externalTransition().from(INITIALIZATION).to(JOB_RUN)
          .on(ToJobRun).callMethod("fromInitToJobRun");
      builder.onEntry(JOB_RUN).callMethod("fromAnyToAny");

      // Any -> JobShow
      builder.externalTransition().from(INITIALIZATION).to(JOB_SHOW)
          .on(ToJobShow).callMethod("fromAnyToAny");
      builder.onEntry(JOB_SHOW).callMethod("entryToJobShow");

      // Any -> Dashboard
      builder.externalTransition().from(INITIALIZATION).to(DASHBOARD)
          .on(ToDashboard).callMethod("fromAnyToAny");
      builder.onEntry(DASHBOARD).callMethod("entryToDashboard");

      // Any -> ShowUsage
      builder.externalTransitions().fromAmong(INITIALIZATION, DASHBOARD).to(SHOW_USAGE)
          .on(ToShowUsage).callMethod("fromAnyToAny");
      builder.onEntry(SHOW_USAGE).callMethod("entryToShowUsage");

      submarineStateMachine = builder.newStateMachine(INITIALIZATION,
          StateMachineConfiguration.create().enableDebugMode(true));
      submarineStateMachine.start();
    } catch (Exception e) {
      e.printStackTrace();
      LOGGER.error("createSubmarineStateMachine: " + e.getMessage());
    }
  }

  @Test
  public void test1() {
    SubmarineStateMachineState state = submarineStateMachine.getCurrentState();
    submarineStateMachine.fire(ToShowUsage);

    SubmarineStateMachineState state2 = submarineStateMachine.getCurrentState();

    int n = 0;
  }

}
