package org.shadowmask.engine.hive.udf;

import java.sql.Timestamp;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.io.IntWritable;

import org.shadowmask.core.mask.rules.MaskEngine;
import org.shadowmask.core.mask.rules.TimestampStrategy;

/**
 * UDFTimestamp.
 *
 */
@Description(name = "timestamp",
             value = "_FUNC_(timestamp, mask) - returns the masked value of timestamp\n"
                + "timestamp - original timestamp type with date string and time string\n"
                + "mask - hide the date components or the time components, 1 masked, 0 unmasked",
             extended = "Example:\n")
public class UDFTimestamp extends UDF {

  public TimestampWritable evaluate(TimestampWritable timestamp, IntWritable mask) {
    if (timestamp == null || mask == null) return null;
    int mode = mask.get();
    Timestamp ts = timestamp.getTimestamp();
    TimestampWritable result = new TimestampWritable();

    MaskEngine me = new MaskEngine(new TimestampStrategy());
    result.set(Timestamp.valueOf(me.evaluate(ts.toString(), mode)));
    return result;
  }

}
