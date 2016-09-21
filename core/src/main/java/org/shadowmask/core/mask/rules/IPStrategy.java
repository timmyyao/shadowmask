package org.shadowmask.core.mask.rules;

import org.apache.commons.lang.StringUtils;

public class IPStrategy extends MaskEngineStrategy {
  public String evaluate(String ip, int mask) {
    String sMask = Integer.toBinaryString(mask);
    char [] cMask = sMask.toCharArray();
    String [] subs = ip.split("\\.");

    if (subs.length < cMask.length) {
      throw new RuntimeException("Please input correct mask code!");
    }
    int distance = subs.length - cMask.length;

    for (int i = 0; i < cMask.length; i++) {
      if (cMask[i] == '1') {
        subs[distance + i] = "0";
      }
    }

    return StringUtils.join(subs, ".");
  }

}
