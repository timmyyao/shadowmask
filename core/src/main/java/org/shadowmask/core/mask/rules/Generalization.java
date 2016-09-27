/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.shadowmask.core.mask.rules;

/**
 * Generalization :
 * "evaluate(data,mode,unit) - returns the generalization value for data\n"
 * "mode - generalization method: 0 for floor, 1 for ceil\n"
 * "unit - generalization unit: must be a positive integer"
 */
public class Generalization {
  private static void checkParameter(int mode, int unit) {
    if (mode != 0 && mode != 1) {
      throw new RuntimeException("mode must be 0 or 1");
    }
    if (unit <= 0) {
      throw new RuntimeException("unit must be a positive integer");
    }
  }

  /**
   * Double version
   */
  public static double evaluate(double data, int mode, int unit) {
    double result = 0;
    checkParameter(mode, unit);
    // floor
    if (mode == 0) {
      result = Math.floor(data/unit)*unit;
    }
    // ceil
    else {
      result = Math.ceil(data/unit)*unit;
    }
    return result;
  }

  /**
   * Integer version
   */
  public static int evaluate(int data, int mode, int unit) {
    int result = 0;
    // calculate the result in long type
    long resultLong = evaluate((long)data, mode, unit);
    // type transform with boundary detection
    if (resultLong > (long) Integer.MAX_VALUE) {
      result = Integer.MAX_VALUE;
    } else if (resultLong < (long) Integer.MIN_VALUE) {
      result = Integer.MIN_VALUE;
    } else {
      result = (int) resultLong;
    }
    return result;
  }

  /**
   * Float version
   */
  public static float evaluate(float data, int mode, int unit) {
    float result = (float) evaluate((double)data, mode, unit);
    return result;
  }

  /**
   * Byte version
   */
  public static byte evaluate(byte data, int mode, int unit) {
    byte result = 0;
    // calculate the result in long type
    long resultLong = evaluate((long)data, mode, unit);
    // type transform with boundary detection
    if(resultLong > (long) Byte.MAX_VALUE) {
      result = Byte.MAX_VALUE;
    }
    else if(resultLong < (long) Byte.MIN_VALUE) {
      result = Byte.MIN_VALUE;
    }
    else {
      result = (byte) resultLong;
    }
    return result;
  }

  /**
   * Long version
   */
  public static long evaluate(long data, int mode, int unit) {
    long result = 0;
    checkParameter(mode,unit);
    result = data / unit;
    result *= unit;
    long remainder = data % unit;
    // floor
    if(mode == 0) {
      if(remainder < 0) {
        if(result - Long.MIN_VALUE < unit) { //result will exceeds MIN_VALUE after floor operation
          result = Long.MIN_VALUE;
        }
        else {
          result -= unit;
        }
      }
    }
    // ceil
    else {
      if(remainder > 0) {
        if(Long.MAX_VALUE - result < unit) { //result will exceeds MAX_VALUE after ceil operation
          result = Long.MAX_VALUE;
        }
        else {
          result += unit;
        }
      }
    }
    return result;
  }

  /**
   * Short version
   */
  public static short evaluate(short data, int mode, int unit) {
    short result = 0;
    // calculate the result in long type
    long resultLong = evaluate((long)data, mode, unit);
    // type transform with boundary detection
    if(resultLong > (long) Short.MAX_VALUE) {
      result = Short.MAX_VALUE;
    }
    else if(resultLong < (long) Short.MIN_VALUE) {
      result = Short.MIN_VALUE;
    }
    else {
      result = (short) resultLong;
    }
    return result;
  }
}
