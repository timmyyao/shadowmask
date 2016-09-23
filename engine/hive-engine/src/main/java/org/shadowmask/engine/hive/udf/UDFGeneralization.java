package org.shadowmask.engine.hive.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.shadowmask.core.mask.rules.Generalization;


/**
 * UDFGeneralization
 */
@Description(name = "generalization",
        value = "_FUNC_(data,mode,unit) - returns the generalization value for data\n"
                + "mode - generalization method: 0 for floor, 1 for ceil\n"
                + "unit - generalization unit: must be a positive integer",
        extended = "Example:\n")
public class UDFGeneralization extends UDF {
  /**
   * Integer version
   */
  public IntWritable evaluate(IntWritable data, IntWritable mode, IntWritable unit) {
    if (data == null) {
      return null;
    }
    int dataVal = data.get();
    int modeVal = mode.get();
    int unitVal = unit.get();
    IntWritable result = new IntWritable(Generalization.evaluate(dataVal, modeVal, unitVal));
    return result;
  }

  /**
   * Double version
   */
  public DoubleWritable evaluate(DoubleWritable data, IntWritable mode, IntWritable unit) {
    if (data == null) {
      return null;
    }
    double dataVal = data.get();
    int modeVal = mode.get();
    int unitVal = unit.get();
    DoubleWritable result = new DoubleWritable(Generalization.evaluate(dataVal, modeVal, unitVal));
    return result;
  }

  /**
   * Float version
   */
  public FloatWritable evaluate(FloatWritable data, IntWritable mode, IntWritable unit) {
    if (data == null) {
      return null;
    }
    float dataVal = data.get();
    int modeVal = mode.get();
    int unitVal = unit.get();
    FloatWritable result = new FloatWritable(Generalization.evaluate(dataVal, modeVal, unitVal));
    return result;
  }

  /**
   * Byte version
   */
  public ByteWritable evaluate(ByteWritable data, IntWritable mode, IntWritable unit) {
    if (data == null) {
      return null;
    }
    byte dataVal = data.get();
    int modeVal = mode.get();
    int unitVal = unit.get();
    ByteWritable result = new ByteWritable(Generalization.evaluate(dataVal, modeVal, unitVal));
    return result;
  }

  /**
   * Long version
   */
  public LongWritable evaluate(LongWritable data, IntWritable mode, IntWritable unit) {
    if (data == null) {
      return null;
    }
    long dataVal = data.get();
    int modeVal = mode.get();
    int unitVal = unit.get();
    LongWritable result = new LongWritable(Generalization.evaluate(dataVal, modeVal, unitVal));
    return result;
  }

  /**
   * Short version
   */
  public ShortWritable evaluate(ShortWritable data, IntWritable mode, IntWritable unit) {
    if (data == null) {
      return null;
    }
    short dataVal = data.get();
    int modeVal = mode.get();
    int unitVal = unit.get();
    ShortWritable result = new ShortWritable(Generalization.evaluate(dataVal, modeVal, unitVal));
    return result;
  }

}

