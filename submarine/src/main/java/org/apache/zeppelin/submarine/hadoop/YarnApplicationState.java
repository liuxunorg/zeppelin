package org.apache.zeppelin.submarine.hadoop;

// org/apache/hadoop/yarn/api/records/YarnApplicationState.class
public enum YarnApplicationState {
  NEW,
  NEW_SAVING,
  SUBMITTED,
  ACCEPTED,
  RUNNING,
  FINISHED,
  FAILED,
  KILLED;
}
