package org.shadowmask.core.mask.rules;


/**
 * UDFShift.
 * "evaluate(data,direction,digit,mode) - shift 'data' \n"
 * "direction - 0 for right, 1 for left\n"
 * "digit - number of digits for shifting"
 * "mode - 0: fill 0 for left shift (direction = 1) and unsigned shift for right shift (direction = 0);"
 * "1: fill 1 for left shift (direction = 1) and signed shift for right shift (direction = 0)"
 */
public class Shift {
  private enum DataType {
    INTEGER,
    LONG,
    SHORT,
    BYTE
  }

  private static void checkParameter(int direction, int digit, int mode, DataType dataType) {
    if(direction != 0 && direction != 1) {
      throw new RuntimeException("direction must be 0 or 1");
    }
    if(mode != 0 && mode != 1) {
      throw new RuntimeException("mode must be 0 or 1");
    }
    if(dataType == DataType.INTEGER) {
      if(digit >= 32 || digit < 0) {
        throw new RuntimeException("digit must between 0 and 31 for Integer input");
      }
    }
    else if(dataType == DataType.LONG) {
      if(digit >= 64 || digit < 0) {
        throw new RuntimeException("digit must between 0 and 63 for Long input");
      }
    }
    else if(dataType == DataType.SHORT) {
      if(digit >= 16 || digit < 0) {
        throw new RuntimeException("digit must between 0 and 15 for Short input");
      }
    }
    else if(dataType == DataType.BYTE) {
      if(digit >= 8 || digit < 0) {
        throw new RuntimeException("digit must between 0 and 7 for Byte input");
      }
    }
  }

  // int calculate
  private static int shiftCalculate(int data, int direction, int digit, int mode) {
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

  // long calculate
  private static long shiftCalculate(long data, int direction, int digit, int mode) {
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
        case 1: result = (data << digit) + ((1L << digit) - 1); break;
        default:
      }
    }
    return result;
  }

  /**
   * Integer version
   */
  public static int evaluate(int data, int direction, int digit, int mode) {
    checkParameter(direction, digit, mode, DataType.INTEGER);
    int result = shiftCalculate(data, direction, digit, mode);
    return result;
  }

  /**
   * Long version
   */
  public static long evaluate(long data, int direction, int digit, int mode) {
    checkParameter(direction, digit, mode, DataType.LONG);
    long result = shiftCalculate(data, direction, digit, mode);
    return result;
  }

  /**
   * Short version
   */
  public static short evaluate(short data, int direction, int digit, int mode) {
    checkParameter(direction, digit, mode, DataType.SHORT);
    short result;
    if (mode == 1) {
      result = (short) shiftCalculate((int) data, direction, digit, mode);
    }
    // Fill the MSB with zeros when doing unsigned shift
    else {
      int dataInt = (int)data & ((1<<16) - 1);
      result = (short) shiftCalculate(dataInt, direction, digit, mode);
    }
    return result;
  }

  /**
   * Byte version
   */
  public static byte evaluate(byte data, int direction, int digit, int mode) {
    checkParameter(direction, digit, mode, DataType.BYTE);
    byte result;
    if (mode == 1) {
      result = (byte) shiftCalculate((int) data, direction, digit, mode);
    }
    // Fill the MSB with zeros when doing unsigned shift
    else {
      int dataInt = (int)data & ((1<<8) - 1);
      result = (byte) shiftCalculate(dataInt, direction, digit, mode);
    }
    return result;
  }
}
