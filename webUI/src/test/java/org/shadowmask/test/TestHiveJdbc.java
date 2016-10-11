package org.shadowmask.test;

import org.junit.Test;
import org.shadowmask.jdbc.connection.KerberizedHiveConnectionProvider;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

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

    @Test
    public void testGetDatabases() throws SQLException {
        KerberizedHiveConnectionProvider provider = new KerberizedHiveConnectionProvider();
        Connection connection = provider.get();
        PreparedStatement stm = connection.prepareStatement("show databases");
        ResultSet res = stm.executeQuery();
        while (res.next())
            System.out.println(res.getString(1));

        stm = connection.prepareStatement("show tables");
        res = stm.executeQuery();
        while (res.next())
            System.out.println(res.getString(1));


        stm = connection.prepareStatement("show tables testdb");
        res = stm.executeQuery();

        while (res.next())
            System.out.println(res.getString(1));
        assertNotNull(res);
    }


}
