package org.shadowmask.engine.hive.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.shadowmask.core.mask.rules.Shift;

/**
 * UDFShift.
 */
@Description(name = "shift",
        value = "_FUNC_(data,direction,digit,mode) - shift 'data' \n"
                + "direction - 0 for right, 1 for left\n"
                + "digit - number of digits for shifting"
                + "mode - 0: fill 0 for left shift (direction = 1) and unsigned shift for right shift (direction = 0);"
                + "1: fill 1 for left shift (direction = 1) and signed shift for right shift (direction = 0)",
        extended = "Example:\n")
public class UDFShift extends UDF {

  /**
   * Integer version
   */
  public IntWritable evaluate(IntWritable data, IntWritable direction, IntWritable digit, IntWritable mode) {
    if(data == null) {
      return null;
    }

    int dataVal = data.get();
    int directionVal = direction.get();
    int digitVal = digit.get();
    int modeVal = mode.get();

    IntWritable result = new IntWritable(Shift.evaluate(dataVal, directionVal, digitVal, modeVal));

    return result;
  }

  /**
   * Long version
   */
  public LongWritable evaluate(LongWritable data, IntWritable direction, IntWritable digit, IntWritable mode) {
    if(data == null) {
      return null;
    }
    long dataVal = data.get();
    int directionVal = direction.get();
    int digitVal = digit.get();
    int modeVal = mode.get();

    LongWritable result = new LongWritable(Shift.evaluate(dataVal, directionVal, digitVal, modeVal));

    return result;
  }

  /**
   * Short version
   */
  public ShortWritable evaluate(ShortWritable data, IntWritable direction, IntWritable digit, IntWritable mode) {
    if(data == null) {
      return null;
    }
    short dataVal = data.get();
    int directionVal = direction.get();
    int digitVal = digit.get();
    int modeVal = mode.get();

    ShortWritable result = new ShortWritable(Shift.evaluate(dataVal, directionVal, digitVal, modeVal));

    return result;
  }

  /**
   * Byte version
   */
  public ByteWritable evaluate(ByteWritable data, IntWritable direction, IntWritable digit, IntWritable mode) {
    if(data == null) {
      return null;
    }
    byte dataVal = data.get();
    int directionVal = direction.get();
    int digitVal = digit.get();
    int modeVal = mode.get();

    ByteWritable result = new ByteWritable(Shift.evaluate(dataVal, directionVal, digitVal, modeVal));

    return result;
  }
}
