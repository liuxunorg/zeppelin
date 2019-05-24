package org.apache.zeppelin.interpreter.launcher;

import org.apache.zeppelin.conf.ZeppelinConfiguration;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class YarnRemoteInterpreterProcessTest {
  @Before
  public void setUp() {
    for (final ZeppelinConfiguration.ConfVars confVar : ZeppelinConfiguration.ConfVars.values()) {
      System.clearProperty(confVar.getVarName());
    }
  }

  @Test
  public void testLauncher() throws IOException {
    ZeppelinConfiguration zConf = ZeppelinConfiguration.create();
    zConf.setServerKerberosKeytab("keytab_value");
    zConf.setServerKerberosPrincipal("Principal_value");
    zConf.setYarnWebappAddress("http://127.0.0.1");

  }
}
