package org.shadowmask.engine.hive.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import org.shadowmask.core.mask.rules.MaskEngine;
import org.shadowmask.core.mask.rules.MobileStrategy;

/**
 * UDFMobile.
 *
 */
@Description(name = "mobile",
             value = "_FUNC_(mobile, mask) - returns the masked value of mobile\n"
                + "mobile - original 11-digit mobile number combined with three parts \n"
                + "mask - hide the district number or the mobile number: 1 masked, 0 unmasked",
             extended = "Example:\n")
public class UDFMobile extends UDF {

  public Text evaluate(Text mobile, IntWritable mask) {
    if (mobile == null || mask == null) return null;
    int mode = mask.get();
    String str_mobile = mobile.toString();

    Text result = new Text();
    MaskEngine me = new MaskEngine(new MobileStrategy());
    result.set(me.evaluate(str_mobile, mode));
    return result;
  }
}
