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

import java.sql.Timestamp;
import java.lang.StringBuilder;

public class TimestampStrategy extends MaskEngineStrategy {
  // Timestamp type limits month in 00~11
  private static final String[] DATE = {"1960","01","01"};

  public String evaluate(String timestamp, int mask) {
    String sMask = Integer.toBinaryString(mask);
    char [] cMask = sMask.toCharArray();
    String [] date_time = timestamp.split("\\x20|-|:");

    if (date_time.length < cMask.length) {
      throw new RuntimeException("Please input correct mask code!");
    }

    int distance = date_time.length - cMask.length;
    for (int i = 0; i < cMask.length; i++) {
      if (cMask[i] == '1') {
        if (distance + i < 3) {
          date_time[distance + i] = DATE[distance + i];
        } else if (cMask[i] == '1') {
          date_time[distance + i] = date_time[distance + i].replaceAll("\\d", "0");
        }
      }
    }
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < date_time.length; i++) {
      if (i < 2) sb.append(date_time[i] + "-");
      if (i == 2) sb.append(date_time[i] + " ");
      if (i > 2 && i < date_time.length - 1) sb.append(date_time[i] + ":");
      if (i == date_time.length - 1) sb.append(date_time[i]);
    }

    return sb.toString();
  }

}
