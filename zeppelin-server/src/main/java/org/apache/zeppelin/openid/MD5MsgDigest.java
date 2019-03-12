package org.apache.zeppelin.openid;

import java.io.File;
import java.io.FileInputStream;
import java.nio.charset.Charset;
import java.security.MessageDigest;

/**
 *
 */
public class MD5MsgDigest {
  public MD5MsgDigest() {
  }

  public static String digest(String rawString) {
    return digest(rawString, "utf-8");
  }

  public static String digest(String rawString, String charset) {
    Charset cs = Charset.forName(charset);

    try {
      return compute(rawString, cs);
    } catch (Exception var4) {
      return "";
    }
  }

  private static String compute(String inStr, Charset charset) throws Exception {
    MessageDigest md5 = MessageDigest.getInstance("MD5");
    byte[] md5Bytes = md5.digest(inStr.getBytes(charset));
    return toHexString(md5Bytes);
  }

  public static String md5file(File file) throws Exception {
    FileInputStream input = new FileInputStream(file);

    try {
      MessageDigest md5 = MessageDigest.getInstance("MD5");
      byte[] buf = new byte[4096];
      boolean var4 = false;

      int len;
      while ((len = input.read(buf)) > 0) {
        md5.update(buf, 0, len);
      }

      String var5 = toHexString(md5.digest());
      return var5;
    } finally {
      input.close();
    }
  }

  public static String md5Bytes(byte[] bytes) throws Exception {
    MessageDigest md5 = MessageDigest.getInstance("MD5");
    md5.update(bytes);
    return toHexString(md5.digest());
  }

  public static String toHexString(byte[] bytes) {
    StringBuffer hexValue = new StringBuffer();

    for (int i = 0; i < bytes.length; ++i) {
      int val = bytes[i] & 255;
      if (val < 16) {
        hexValue.append("0");
      }

      hexValue.append(Integer.toHexString(val));
    }

    return hexValue.toString();
  }
}
