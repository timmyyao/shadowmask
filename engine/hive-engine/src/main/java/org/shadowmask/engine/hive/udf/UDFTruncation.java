package org.shadowmask.engine.hive.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.shadowmask.core.mask.rules.Truncation;

/**
 * UDFTruncation
 */
@Description(name = "truncation",
        value = "_FUNC_(data,length,mode) - returns a sub-Text with 'length' digits\n"
                + "mode - truncation method (0 for reserving the first half, 1 for reserving the second half)\n",
        extended = "Example:\n")
public class UDFTruncation extends UDF{
  private final Text result = new Text();

  public Text evaluate(Text data, IntWritable length, IntWritable mode) {
    // returns null when input is null
    if(data == null) {
      return null;
    }
    String dataStr = data.toString();
    int lengthVal = length.get();
    int modeVal = mode.get();
    result.set(Truncation.evaluate(dataStr, lengthVal, modeVal));
    return result;
  }
}
