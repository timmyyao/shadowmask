package org.shadowmask.core.mask.rules;

import java.util.Arrays;

/**
 * Mask :
 * "evaluate(data,start,end,tag) - replace the sub-string of 'data' from start to end with 'tag'\n"
 * "start - start position (sequence count from 1)\n"
 * "end - end position (end >= start)\n"
 * "tag - the signal to replace the sub-string which must be one single character"
 */
public class Mask {
  public static String evalutate(String data, int start, int end, String tag) {
    String result;
    // throw exception when 'tag' is invalid (not one single character)
    if(tag.length() != 1) {
      throw new RuntimeException("tag must be one single character");
    }
    char ch = tag.charAt(0);
    // throw exception if 'start' or 'end' is invalid (startVal > endVal)
    if(start > end) {
      throw new RuntimeException("start must <= end");
    }
    // returns entire data when the period [start,end] exceeds the scope of 'data'
    if(start > data.length() || end < 1) {
      result = new String(data);
      return result;
    }
    // returns expected result: combine head, maskStr and tail
    String head = new String();
    String tail = new String();
    String maskStr;
    if (start > 1) {
      head = data.substring(0, start - 1);
    }
    else {
      start = 1;
    }
    if (end < data.length()) {
      tail = data.toString().substring(end, data.length());
    }
    else {
      end = data.length();
    }
    char[] mask = new char[end - start + 1];
    Arrays.fill(mask, ch);
    maskStr = new String(mask);
    result = head + maskStr + tail;
    return result;
  }
}
