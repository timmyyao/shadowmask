/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.shadowmask.core.discovery.rules;

import org.shadowmask.core.discovery.DataDiscoveryException;
import org.shadowmask.core.discovery.RuleContext;

/**
 * This rule would evaluate whether the column value is a date.
 */
public class DateRule extends QusiIdentifierRule {
  public DateRule(RuleContext ruleContext) {super(ruleContext);}

  @Override public boolean evaluate() {
    if (value == null) {
      throw new DataDiscoveryException(
          "Should fill the column value before fire inspect rules.");
    }
    String subs[] = value.split("-");
    if (subs.length != 3) {
      return false;
    }
    int subsValue[] = new int[3];
    try {
      subsValue[0] = Integer.decode(subs[0]);
      subsValue[1] = Integer.decode(subs[1]);
      subsValue[2] = Integer.decode(subs[2]);
    } catch (NumberFormatException e) {
      return false;
    }
    // Requirements of date "year-month-day" : year>1900, 1<=month<=12, 1<=day<=31
    if (subsValue[0] < 1901 || subsValue[1] < 1 || subsValue[1] > 12 || subsValue[2] < 1 || subsValue[2] > 31) {
      return false;
    }
    return  true;
  }
}
