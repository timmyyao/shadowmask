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

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

public class UDAFLDObject extends UDAFObject {
  private String sensitive_value;
  private Map<String, Integer> deversities;
  public UDAFLDObject(String code, String value) {
    super(code);
    this.sensitive_value = value;
    this.deversities = new HashMap<String, Integer>();
    this.deversities.put(sensitive_value, 1);
  }

  @Override
  public int hashCode() {
    int hash = 0;
    hash = row_key.hashCode();
    return hash;
  }

  @Override
  public boolean equals(Object obj) {
    if(this == obj) {
      return true;
    }
    if(obj == null || getClass() != obj.getClass()) {
        return false;
    }
    UDAFLDObject row = (UDAFLDObject) obj;
    if(!row_key.equals(row.row_key)) return false;
    return true;
  }

  @Override
  public String getRow() {
    return row_key;
  }

  @Override
  public Integer getCount() {
    return deversities.size();
  }

  public String getSensitiveValue() {
    return sensitive_value;
  }

  public HashMap<String, Integer> getDeversities() {
    return (HashMap<String, Integer>) deversities;
  }

}
