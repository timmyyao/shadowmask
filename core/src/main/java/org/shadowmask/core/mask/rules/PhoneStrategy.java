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

public class PhoneStrategy extends MaskEngineStrategy {
  public String evaluate(String phone, int mask) {
    if(mask < 0 || mask >3) {
      throw new RuntimeException("mask code for Phone must between 0 and 3!");
    }
    String result = "";
    switch(mask) {
      case 0:
        result = phone;
        break;
      case 1:
        result = phone.substring(0, phone.indexOf('-'));
        break;
      case 2:
        result = phone.substring(phone.indexOf('-') + 1, phone.length());
        break;
      case 3:
        break;
    }
    return result;
  }

}
