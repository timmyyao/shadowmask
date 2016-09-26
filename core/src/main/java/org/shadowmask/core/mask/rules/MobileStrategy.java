package org.shadowmask.core.mask.rules;

import org.apache.commons.lang.StringUtils;

public class MobileStrategy extends MaskEngineStrategy {
  public String evaluate(String mobile, int mask) {
    String sMask = Integer.toBinaryString(mask);
    char [] cMask = sMask.toCharArray();
    String [] subs = new String[3];
    if (subs.length < cMask.length) {
      throw new RuntimeException("Please input correct mask code!");
    }
    int distance = subs.length - cMask.length;

    subs[0] = mobile.substring(0, 3);
    subs[1] = mobile.substring(3, 7);
    subs[2] = mobile.substring(7, 11);

    for (int i = 0; i < cMask.length; i++) {
      if (cMask[i] == '1') {
        subs[distance + i] = subs[distance + i].replaceAll("\\d", "*");
      }
    }

    return StringUtils.join(subs, "");
  }

}
