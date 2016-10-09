package org.shadowmask.jdbc.connection;

import java.sql.Connection;
import java.util.function.Supplier;

/**
 * Created by liyh on 16/10/9.
 */
public interface ConnectionProvider extends Supplier<Connection> {
}
