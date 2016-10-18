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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * Executed jdbc  task .
 */
public abstract class ExecutedJdbcTask extends JDBCTask {
  Logger logger = Logger.getLogger(this.getClass());

  public abstract RollbackableProcedureWatcher watcher();

  @Override public void invoke() {
    Connection connection = null;
    try {
      if (watcher() != null) {
        watcher().preStart();
      }
      PreparedStatement stm = connection.prepareStatement(sql());
      stm.execute();
      connection.commit();
      if (watcher() != null) {
        watcher().onComplete();
      }
    } catch (Exception e) {
      logger.warn(
          String.format("Exception occurred when execute sql[ %s ]", sql()), e);
      if (watcher() != null) {
        watcher().onException(e);
      }
      if (rollbackAble()) {
        try {
          if (watcher() != null) {
            watcher().onRollbackStart();
          }
          connection.rollback();
          if (watcher() != null) {
            watcher().onRollBackCompeleted();
          }
        } catch (SQLException e1) {
          if (watcher() != null) {
            watcher().onRollBackException(e1);
            logger.warn(String
                    .format("Exception occurred when rollback [ %s ]", connection),
                e);
          }
        }
      }
    } finally {
      if (connection != null)
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
