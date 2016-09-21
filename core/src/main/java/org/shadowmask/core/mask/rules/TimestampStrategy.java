package org.shadowmask.core.mask.rules;

import java.sql.Timestamp;
import java.lang.StringBuilder;

public class TimestampStrategy extends MaskEngineStrategy {
  // Timestamp type limits month in 00~11
  private static final String[] DATE = {"1960","01","01"};

  public String evaluate(String timestamp, int mask) {
    String sMask = Integer.toBinaryString(mask);
    char [] cMask = sMask.toCharArray();
    String [] date_time = timestamp.split("\\x20|-|:");

    if (date_time.length < cMask.length) {
      throw new RuntimeException("Please input correct mask code!");
    }

    int distance = date_time.length - cMask.length;
    for (int i = 0; i < cMask.length; i++) {
      if (cMask[i] == '1') {
        if (distance + i < 3) {
          date_time[distance + i] = DATE[distance + i];
        } else if (cMask[i] == '1') {
          date_time[distance + i] = date_time[distance + i].replaceAll("\\d", "0");
        }
      }
    }
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < date_time.length; i++) {
      if (i < 2) sb.append(date_time[i] + "-");
      if (i == 2) sb.append(date_time[i] + " ");
      if (i > 2 && i < date_time.length - 1) sb.append(date_time[i] + ":");
      if (i == date_time.length - 1) sb.append(date_time[i]);
    }

    return sb.toString();
  }

}
