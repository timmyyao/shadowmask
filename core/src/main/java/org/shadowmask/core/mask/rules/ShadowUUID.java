package org.shadowmask.core.mask.rules;

import java.util.UUID;

import org.shadowmask.core.mask.rules.ShadowUID;

public class ShadowUUID extends ShadowUID {

  /* Generate UID by method-1: use JAVA UUID. More efficient.*/
  public String evaluate(String reference) {
    byte[] bytes = reference.getBytes();
    UUID uuid = UUID.nameUUIDFromBytes(bytes);
    String res = uuid.toString().replaceAll("\\-", "");
    return res;
  }

}
