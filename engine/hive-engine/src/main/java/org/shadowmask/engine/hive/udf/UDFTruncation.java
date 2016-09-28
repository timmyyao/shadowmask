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
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.shadowmask.core.mask.rules.Truncation;

/**
 * UDFTruncation
 */
@Description(name = "truncation",
        value = "_FUNC_(data,length,mode) - returns a sub-Text with 'length' digits\n"
                + "mode - truncation method (0 for reserving the first half, 1 for reserving the second half)\n",
        extended = "Example:\n")
public class UDFTruncation extends UDF{
  private final Text result = new Text();

  public Text evaluate(Text data, IntWritable length, IntWritable mode) {
    // returns null when input is null
    if(data == null) {
      return null;
    }
    String dataStr = data.toString();
    int lengthVal = length.get();
    int modeVal = mode.get();
    result.set(Truncation.evaluate(dataStr, lengthVal, modeVal));
    return result;
  }
}
