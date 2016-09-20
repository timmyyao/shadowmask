package org.shadowmask.engine.hive.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;

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
  private int evaluate(int data, int direction, int digit, int mode) {
    int result = data;
    // right shift
    if(direction == 0) {
      switch (mode) {
        // unsigned right shift
        case 0: result = data >>> digit; break;
        // signed right shift
        case 1: result = data >> digit; break;
        default:
      }
    }
    // left shift
    else {
      switch (mode) {
        // fill 0
        case 0: result = data << digit; break;
        // fill 1
        case 1: result = (data << digit) + ((1 << digit) - 1); break;
        default:
      }
    }
    return result;
  }

  private long evaluate(long data, int direction, int digit, int mode) {
    long result = data;
    // right shift
    if(direction == 0) {
      switch (mode) {
        // unsigned right shift
        case 0: result = data >>> digit; break;
        // signed right shift
        case 1: result = data >> digit; break;
        default:
      }
    }
    // left shift
    else {
      switch (mode) {
        // fill 0
        case 0: result = data << digit; break;
        // fill 1
        case 1: result = data << digit + 1 << digit - 1; break;
        default:
      }
    }
    return result;
  }

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
    if(directionVal != 0 && directionVal != 1) {
      throw new RuntimeException("direction must be 0 or 1");
    }
    if(modeVal != 0 && modeVal != 1) {
      throw new RuntimeException("mode must be 0 or 1");
    }
    IntWritable result = new IntWritable();
    if(digitVal >= 32 || digitVal < 0) {
      throw new RuntimeException("digit must between 0 and 31 for Integer input");
    }
    result.set(evaluate(dataVal,directionVal,digitVal,modeVal));
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
    if(directionVal != 0 && directionVal != 1) {
      throw new RuntimeException("direction must be 0 or 1");
    }
    if(modeVal != 0 && modeVal != 1) {
      throw new RuntimeException("mode must be 0 or 1");
    }
    LongWritable result = new LongWritable();
    if(digitVal >= 64 || digitVal < 0) {
      throw new RuntimeException("digit must between 0 and 63 for Long input");
    }
    result.set(evaluate(dataVal,directionVal,digitVal,modeVal));
    return result;
  }

  /**
   * Short version
   */
  public ShortWritable evaluate(ShortWritable data, IntWritable direction, IntWritable digit, IntWritable mode) {
    if(data == null) {
      return null;
    }
    int dataVal = data.get();
    int directionVal = direction.get();
    int digitVal = digit.get();
    int modeVal = mode.get();
    if(directionVal != 0 && directionVal != 1) {
      throw new RuntimeException("direction must be 0 or 1");
    }
    if(modeVal != 0 && modeVal != 1) {
      throw new RuntimeException("mode must be 0 or 1");
    }
    ShortWritable result = new ShortWritable();
    if(digitVal >= 16 || digitVal < 0) {
      throw new RuntimeException("digit must between 0 and 15 for Short input");
    }
    result.set((short)evaluate(dataVal,directionVal,digitVal,modeVal));
    return result;
  }

  /**
   * Byte version
   */
  public ByteWritable evaluate(ByteWritable data, IntWritable direction, IntWritable digit, IntWritable mode) {
    if(data == null) {
      return null;
    }
    int dataVal = data.get();
    int directionVal = direction.get();
    int digitVal = digit.get();
    int modeVal = mode.get();
    if(directionVal != 0 && directionVal != 1) {
      throw new RuntimeException("direction must be 0 or 1");
    }
    if(modeVal != 0 && modeVal != 1) {
      throw new RuntimeException("mode must be 0 or 1");
    }
    ByteWritable result = new ByteWritable();
    if(digitVal >= 8 || digitVal < 0) {
      throw new RuntimeException("digit must between 0 and 7 for Byte input");
    }
    result.set((byte)evaluate(dataVal,directionVal,digitVal,modeVal));
    return result;
  }
}
