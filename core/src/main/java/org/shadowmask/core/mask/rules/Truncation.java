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
 * Truncation :
 * "evaluate(data,length,mode) - returns a sub-string with 'length' digits\n"
 * "mode - truncation method (0 for reserving the first half, 1 for reserving the second half)\n"
 */
public class Truncation {
  public static String evaluate(String data, int length, int mode) {
    String result;
    // returns null when the value of 'mode' or 'length' is invalid
    if(mode != 0 && mode != 1) {
      throw new RuntimeException("mode must be 0 or 1");
    }
    if(length < 0) {
      throw new RuntimeException("length must be a positive integer");
    }
    // returns entire data when 'length' exceeds the length of data
    if(length >= data.length()) {
      result = new String(data);
      return result;
    }
    // returns the expected result
    if(mode == 0) {
      result = data.substring(0, length);
    }
    else {
      result = data.substring(data.length()-length, data.length());
    }
    return result;
  }
}

