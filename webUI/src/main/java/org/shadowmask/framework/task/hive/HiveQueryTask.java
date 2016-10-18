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
package org.shadowmask.framework.task.hive;

import org.shadowmask.jdbc.connection.ConnectionProvider;
import org.shadowmask.jdbc.connection.WrappedConnectionProvider;
import org.shadowmask.framework.task.ProcedureWatcher;
import org.shadowmask.framework.task.QueryJdbcTask;

import java.io.Serializable;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;

public abstract class HiveQueryTask<T extends Serializable>
    extends QueryJdbcTask<T> {

  List<T> res = null;

  // must be assigned at slave-end

  ProcedureWatcher watcher;

  ConnectionProvider connectionProvider;

  @Override public void setUp() {
    super.setUp();
    watcher = new Watcher();
    res = new ArrayList<>();
    connectionProvider = WrappedConnectionProvider.getInstance();
  }

  @Override public void collect(T t) {
    res.add(t);
  }

  @Override public List<T> queryResults() {
    return res;
  }

  @Override public ProcedureWatcher watcher() {
    return watcher;
  }

  @Override public Connection connectDB() {
    if (connectionDesc() != null)
      return connectionProvider.get(connectionDesc());
    else
      return connectionProvider.get();
  }

  class Watcher implements ProcedureWatcher {

    @Override public void preStart() {
      // do nothing
    }

    @Override public void onComplete() {
      // do nothing
    }

    @Override public void onException(Exception e) {
      //do nothing
    }
  }

}
