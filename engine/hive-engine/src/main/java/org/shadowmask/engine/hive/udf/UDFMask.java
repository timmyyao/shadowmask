package org.shadowmask.engine.hive.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import java.util.Arrays;

/**
 * UDFMask.
 */
@Description(name = "mask",
        value = "_FUNC_(data,start,end,tag) - replace the sub-text of 'data' from start to end with 'tag'\n"
                + "start - start position (sequence count from 1)\n"
                + "end - end position (end >= start)\n"
                + "tag - the signal to replace the sub-text which must be one single character",
        extended = "Example:\n")
public class UDFMask extends UDF{
  private final Text result = new Text();

  public Text evaluate(Text data, IntWritable start, IntWritable end, Text tag) {
    // returns null when input is null
    if(data == null){
      return null;
    }
    // throw exception when 'tag' is invalid (not one single character)
    String dataStr = data.toString();
    String tagStr = tag.toString();
    if(tagStr.length() != 1) {
      throw new RuntimeException("tag must be one single character");
    }
    char ch = tagStr.charAt(0);
    int startVal = start.get();
    int endVal = end.get();
    // throw exception if 'startVal' or 'endVal' is invalid (startVal > endVal)
    if(startVal > endVal) {
      throw new RuntimeException("start must <= end");
    }
    // returns entire data when the period [start,end] exceeds the scope of 'data'
    if(startVal > dataStr.length() || endVal < 1) {
      result.set(data);
      return result;
    }
    // returns expected result: combine head, maskStr and tail
    String head = new String();
    String tail = new String();
    String maskStr;
    if (startVal > 1) {
      head = dataStr.substring(0, startVal - 1);
    }
    else {
      startVal = 1;
    }
    if (endVal < dataStr.length()) {
      tail = data.toString().substring(endVal, dataStr.length());
    }
    else {
      endVal = dataStr.length();
    }
    char[] mask = new char[endVal - startVal + 1];
    Arrays.fill(mask, ch);
    maskStr = new String(mask);
    result.set(head + maskStr + tail);

    return result;
  }
}
