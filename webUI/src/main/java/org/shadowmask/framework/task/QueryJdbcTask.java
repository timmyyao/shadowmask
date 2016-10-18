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

package org.shadowmask.framework.task;

import org.apache.log4j.Logger;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

public abstract class QueryJdbcTask<T extends Serializable> extends JDBCTask {
  Logger logger = Logger.getLogger(this.getClass());

  @Override public void setUp() {
  }

  @Override public boolean rollbackAble() {
    return false;
  }

  /**
   * the way transform a result to T .
   *
   * @return
   */
  public abstract JdbcResultCollector<T> collector();

  /**
   * collect result by collector .
   *
   * @param t
   */
  public abstract void collect(T t);

  /**
   * execution result .
   *
   * @return
   */
  public abstract List<T> queryResults();

  /**
   * watch the query  procedure .
   *
   * @return
   */
  public abstract ProcedureWatcher watcher();

  @Override public void invoke() {
    Connection connection = null;
    PreparedStatement stm = null;
    try {
      if (watcher() != null) {
        watcher().preStart();
      }
      connection = connectDB();
      stm = connection.prepareStatement(sql());
      ResultSet resultSet = stm.executeQuery();
      if (resultSet != null) {
        while (resultSet.next()) {
          collect(collector().collect(resultSet));
        }
      }
      if (watcher() != null) {
        watcher().onComplete();
      }
    } catch (SQLException e) {
      if (watcher() != null) {
        watcher().onException(e);
      }
      logger.warn(
          String.format("Exception occurred when execute sql[ %s ]", sql()), e);
    } finally {
      if (stm != null) {
        try {
          stm.close();
        } catch (SQLException e) {
          logger.warn(String
              .format("Exception occurred when close statement[ %s ]", stm), e);
        }
      }
      if (connection != null) {
        try {
          connection.close();
        } catch (SQLException e) {
          logger.warn(String
              .format("Exception occurred when release connection[ %s ]",
                  connection), e);
        }
      }
    }
  }
}
