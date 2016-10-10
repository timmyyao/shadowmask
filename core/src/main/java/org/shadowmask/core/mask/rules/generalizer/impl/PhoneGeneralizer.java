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

package org.shadowmask.core.mask.rules.generalizer.impl;

import org.apache.commons.lang3.StringUtils;
import org.shadowmask.core.mask.rules.MaskRuntimeException;
import org.shadowmask.core.mask.rules.generalizer.Generalizer;

public class PhoneGeneralizer implements Generalizer<String, String> {
  private static int ROOT_HIERARCHY_LEVEL = 2;

  @Override
  public String generalize(String phone, int hierarchyLevel) {
    if (phone == null) {
      return null;
    }

    if (hierarchyLevel > ROOT_HIERARCHY_LEVEL || hierarchyLevel < 0) {
      throw new MaskRuntimeException("Root hierarchy level of MobileGeneralizer is " + ROOT_HIERARCHY_LEVEL +
              ", invalid input hierarchy level[" + hierarchyLevel + "]");
    }

    if (hierarchyLevel == 0) {
      return phone;
    }

    if (phone.length() != 11) {
      throw new MaskRuntimeException("Invalid phone number to generalize:" + phone);
    }

    String [] subs = phone.split("\\-");

    for (int i = 1; i < hierarchyLevel; i++) {
      subs[ROOT_HIERARCHY_LEVEL - hierarchyLevel] = subs[ROOT_HIERARCHY_LEVEL - hierarchyLevel].replaceAll("\\d", "*");
    }

    return StringUtils.join(subs, "-");
  }

  @Override
  public int getRootLevel() {
    return ROOT_HIERARCHY_LEVEL;
  }
}
