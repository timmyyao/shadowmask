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

public class AgeRule extends QusiIdentifierRule {

  public AgeRule(RuleContext ruleContext) {
    super(ruleContext);
  }

  @Override public boolean evaluate() {
    if (value == null) {
      throw new DataDiscoveryException(
          "Should fill the column value before fire inspect rules.");
    }

    if (name == null) {
      return false;
    } else if (name.equalsIgnoreCase("age")) {
      return true;
    } else {
      return false;
    }
  }
}
