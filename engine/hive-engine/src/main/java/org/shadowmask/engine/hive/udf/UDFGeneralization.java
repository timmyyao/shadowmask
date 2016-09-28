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

