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
import org.shadowmask.core.mask.rules.Mask;


/**
 * UDFMask.
 */
@Description(name = "mask",
        value = "_FUNC_(data,start,end,tag) - replace the sub-text of 'data' from start to end with 'tag'\n"
                + "start - start position (sequence count from 1)\n"
                + "end - end position (end >= start)\n"
                + "tag - the signal to replace the sub-text which must be one single character",
        extended = "Example:\n")
public class UDFMask extends UDF{
  private final Text result = new Text();

  public Text evaluate(Text data, IntWritable start, IntWritable end, Text tag) {
    // returns null when input is null
    if(data == null){
      return null;
    }

    result.set(Mask.evalutate(data.toString(), start.get(), end.get(), tag.toString()));
    return result;
  }
}
