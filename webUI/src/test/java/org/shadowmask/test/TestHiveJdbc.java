package org.shadowmask.test;

import org.junit.Test;
import org.shadowmask.jdbc.connection.KerberizedHiveConnectionProvider;

import java.sql.Connection;
import static org.junit.Assert.*;

/**
 * Created by liyh on 16/10/9.
 */
public class TestHiveJdbc {

    @Test
    public void testGetKerberosConnection(){
        KerberizedHiveConnectionProvider provider = new KerberizedHiveConnectionProvider();
        Connection connection = provider.get();
        assertNotNull(connection);
    }
}
