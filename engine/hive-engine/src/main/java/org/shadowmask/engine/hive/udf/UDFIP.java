package org.shadowmask.engine.hive.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import org.shadowmask.core.mask.rules.MaskEngine;
import org.shadowmask.core.mask.rules.IPStrategy;

/**
 * UDFIP.
 *
 */
@Description(name = "ip",
             value = "_FUNC_(ip, mask) - returns the masked value of ip\n"
                + "ip - original ip like x.x.x.x\n"
                + "mask - flags to imply hiding or displaying: 1 masked, 0 unmasked",
             extended = "Example:\n")
public class UDFIP extends UDF {

  public Text evaluate(Text ip, IntWritable mask) {
    if (ip == null || mask == null) return null;
    int mode = mask.get();
    String str_ip = ip.toString();

    Text result = new Text();
    MaskEngine me = new MaskEngine(new IPStrategy());
    result.set(me.evaluate(str_ip, mode));
    return result;
  }
}
