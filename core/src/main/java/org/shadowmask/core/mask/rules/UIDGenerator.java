package org.shadowmask.core.mask.rules;

import org.shadowmask.core.mask.rules.ShadowUUID;
import org.shadowmask.core.mask.rules.ShadowSignitureUID;

public class UIDGenerator {
  public static ShadowUID getDefaultUID() {
    return new ShadowUUID();
  }

  public static ShadowUID getSignitureUID(String method) {
    return new ShadowSignitureUID(method);
  }

}
