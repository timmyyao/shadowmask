package org.shadowmask.utils;

import java.util.ResourceBundle;

/**
 * Created by liyh on 16/10/9.
 */
public class HiveProps {
    public static ResourceBundle bundle  = ResourceBundle.getBundle("jdbc_hive");

    static
    public String url  = bundle.getString("jdbc.hive.url");

    static
    public String driver = bundle.getString("jdbc.hive.driverClass");

    static
    public String user = bundle.getString("jdbc.hive.user");

    static
    public String password = bundle.getString("jdbc.hive.password");

    static
    public String authMethod = bundle.getString("jdbc.hive.auth.method");

    static
    public String krbRealm = bundle.getString("krb.realm");

    static
    public String krbKDC = bundle.getString("krb.kdc");

    static
    public String krbUser = bundle.getString("krb.user");

    static
    public String krbKeytab = bundle.getString("krb.keytab");
}
