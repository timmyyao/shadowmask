package org.shadowmask.engine.hive.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.shadowmask.core.mask.rules.Mask;


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

    result.set(Mask.evalutate(data.toString(), start.get(), end.get(), tag.toString()));
    return result;
  }
}
