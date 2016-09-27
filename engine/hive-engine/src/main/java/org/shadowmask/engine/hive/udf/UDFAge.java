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

package org.shadowmask.engine.hive.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.shadowmask.core.mask.rules.Generalization;

/**
 * UDFAge.
 */
@Description(name = "age",
        value = "_FUNC_(age,mode,unit) - returns the generalization value for age\n"
                + "mode - generalization method: 0 for floor, 1 for ceil\n"
                + "unit - generalization unit: must be a positive integer",
        extended = "Example:\n")
public class UDFAge extends UDF{
  /**
   * Integer version
   */
  public IntWritable evaluate(IntWritable age, IntWritable mode, IntWritable unit) {
    if (age == null) {
      return null;
    }
    int ageVal = age.get();
    int modeVal = mode.get();
    int unitVal = unit.get();
    IntWritable result = new IntWritable(Generalization.evaluate(ageVal, modeVal, unitVal));
    return result;
  }

  /**
   * Byte version
   */
  public ByteWritable evaluate(ByteWritable age, IntWritable mode, IntWritable unit) {
    if (age == null) {
      return null;
    }
    byte ageVal = age.get();
    int modeVal = mode.get();
    int unitVal = unit.get();
    ByteWritable result = new ByteWritable(Generalization.evaluate(ageVal, modeVal, unitVal));
    return result;
  }

  /**
   * Long version
   */
  public LongWritable evaluate(LongWritable age, IntWritable mode, IntWritable unit) {
    if (age == null) {
      return null;
    }
    long ageVal = age.get();
    int modeVal = mode.get();
    int unitVal = unit.get();
    LongWritable result = new LongWritable(Generalization.evaluate(ageVal, modeVal, unitVal));
    return result;
  }

  /**
   * Short version
   */
  public ShortWritable evaluate(ShortWritable age, IntWritable mode, IntWritable unit) {
    if (age == null) {
      return null;
    }
    short ageVal = age.get();
    int modeVal = mode.get();
    int unitVal = unit.get();
    ShortWritable result = new ShortWritable(Generalization.evaluate(ageVal, modeVal, unitVal));
    return result;
  }
}
