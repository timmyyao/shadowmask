package org.shadowmask.core.mask.rules;

import java.lang.StringBuilder;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/* Generate UID by method-2: use MD5/SHA/..*/
public class ShadowSignitureUID extends ShadowUID {
  private String encryptor = "MD5";

  public ShadowSignitureUID(String encryptor) {
    if (encryptor != null) this.encryptor = encryptor;
  }

  public String evaluate(String content) {
    try {
      MessageDigest md = MessageDigest.getInstance(encryptor);
      md.update(content.getBytes());
      byte[] eb = md.digest();
      md.reset();

      StringBuilder sb = new StringBuilder();
      for (byte b : eb) sb.append(Integer.toHexString(b&0xff));

      return sb.toString();
    } catch (NoSuchAlgorithmException e) {
      e.printStackTrace();
      return null;
    }
  }

}
