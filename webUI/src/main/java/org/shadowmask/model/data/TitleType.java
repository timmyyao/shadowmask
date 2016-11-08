/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.shadowmask.model.data;

public enum TitleType {
  ID("ID", "唯一标示符","#02D5A6"),
  HALF_ID("HALF_ID", "半标示符","#F5A623"),
  SENSITIVE("SENSITIVE", "敏感数据","#F76E7F"),
  NONE_SENSITIVE("NONE_SENSITIVE", "非敏感数据","#4A90E2"),
  UNKNOWN("UNKNOWN", "未标记数据","");

  public String name;
  public String desc;
  public String color;

  TitleType(String name, String desc,String color) {
    this.name = name;
    this.desc = desc;
    this.color=color;
  }
}
