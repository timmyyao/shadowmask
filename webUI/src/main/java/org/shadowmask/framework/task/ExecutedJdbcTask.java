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
import org.shadowmask.model.datareader.Command;
import org.shadowmask.utils.NeverThrow;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * Executed jdbc  task .
 */
public abstract class ExecutedJdbcTask<W extends RollbackableProcedureWatcher>
    extends JDBCTask<W> {
  Logger logger = Logger.getLogger(this.getClass());

  @Override public void invoke() {
    Connection connection = null;
    try {
      triggerPreStart();
      PreparedStatement stm = connection.prepareStatement(sql());
      stm.execute();
      connection.commit();
      triggerComplete();
    } catch (Exception e) {
      logger.warn(
          String.format("Exception occurred when execute sql[ %s ]", sql()), e);
      triggerException(e);
      if (rollbackAble()) {
        try {
          triggerPreRollback();
          connection.rollback();
          triggerRollbackCompleted();
        } catch (SQLException e1) {
          triggerRollbackException(e1);
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

  /**
   * trigger preRollback .
   */
  void triggerPreRollback() {
    if (getAllWatchers() != null) {
      for (final W w : getAllWatchers()) {
        NeverThrow.exe(new Command() {
          @Override public void exe() {
            w.onRollbackStart();
          }
        }, new NeverThrow.LoggerConsumer(), null);
      }
    }
  }

  /**
   * trigger rollbackException
   */
  void triggerRollbackException(final Throwable t) {
    if (getAllWatchers() != null) {
      for (final W w : getAllWatchers()) {
        NeverThrow.exe(new Command() {
          @Override public void exe() {
            w.onRollBackException(t);
          }
        }, new NeverThrow.LoggerConsumer(), null);
      }
    }
  }

  /**
   * trigger rollback completed .
   */
  void triggerRollbackCompleted() {
    if (getAllWatchers() != null) {
      for (final W w : getAllWatchers()) {
        NeverThrow.exe(new Command() {
          @Override public void exe() {
            w.onRollBackCompeleted();
          }
        }, new NeverThrow.LoggerConsumer(), null);
      }
    }
  }

}
