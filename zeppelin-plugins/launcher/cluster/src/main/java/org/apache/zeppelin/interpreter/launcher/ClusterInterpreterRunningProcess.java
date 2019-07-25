package org.apache.zeppelin.interpreter.launcher;

import com.google.gson.Gson;
import org.apache.zeppelin.cluster.ClusterManagerServer;
import org.apache.zeppelin.interpreter.remote.RemoteInterpreterRunningProcess;

import java.util.HashMap;
import java.util.Map;

import static org.apache.zeppelin.cluster.ClusterManagerServer.CLUSTER_INTP_EVENT_TOPIC;
import static org.apache.zeppelin.cluster.event.ClusterEvent.CLUSTER_CLOSE_INTP_PROCESS;
import static org.apache.zeppelin.cluster.event.ClusterEventListener.CLUSTER_EVENT;
import static org.apache.zeppelin.cluster.event.ClusterEventListener.CLUSTER_EVENT_MSG;

public class ClusterInterpreterRunningProcess extends RemoteInterpreterRunningProcess {
  private String intpSettingGroupId;
  private String remoteSrvHost;
  private int remoteSrvPort;
  private boolean isRunningOnDocker = false;

  public ClusterInterpreterRunningProcess(String intpSettingName,
                                          int connectTimeout,
                                          String intpTSrvHost,
                                          int intpTSrvPort,
                                          String intpSettingGroupId,
                                          String remoteSrvHost,
                                          int remoteSrvPort,
                                          boolean isRunningOnDocker) {
    super(intpSettingName, connectTimeout, intpTSrvHost, intpTSrvPort);

    this.intpSettingGroupId = intpSettingGroupId;
    this.remoteSrvHost = remoteSrvHost;
    this.remoteSrvPort = remoteSrvPort;
    this.isRunningOnDocker = isRunningOnDocker;
  }

  @Override
  public void stop() {
    super.stop();

    // If it is docker mode, you need to clean up the remote interpreter container.
    if (isRunningOnDocker) {
      Gson gson = new Gson();
      Map<String, Object> mapEvent = new HashMap<>();
      mapEvent.put(CLUSTER_EVENT, CLUSTER_CLOSE_INTP_PROCESS);
      mapEvent.put(CLUSTER_EVENT_MSG, intpSettingGroupId);
      String strEvent = gson.toJson(mapEvent);

      ClusterManagerServer clusterServer = ClusterManagerServer.getInstance();
      clusterServer.unicastClusterEvent(remoteSrvHost, remoteSrvPort,
          CLUSTER_INTP_EVENT_TOPIC, strEvent);
    }
  }
}
