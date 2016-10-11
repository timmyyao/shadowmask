package org.shadowmask.test.hive

import java.io.IOException
import java.sql.{Connection, DriverManager}

import org.apache.commons.dbutils.{DbUtils, QueryRunner}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.security.UserGroupInformation
import org.junit.Assert._
import org.junit.Test

/**
  * Created by liyh on 16/9/21.
  */
class TestDriver {
  @Test
  def getConnection() = {
    System.setProperty("java.security.krb5.realm", "IDC.WANDA-GROUP.NET")
    System.setProperty("java.security.krb5.kdc", "bjs0-3a5.big1.lf.wanda.cn:88")
//    System.setProperty("java.security.krb5.conf", "/Users/liyh/Documents/exchange_app_user.keytab");
    val conf = new Configuration();
    conf.setBoolean( "hadoop.security.authorization" , true);
    conf.set( "hadoop.security.authentication" , "kerberos" );
//    conf.set("kerberosuser", "exchange_app_user@IDC.WANDA-GROUP.NET")
    UserGroupInformation. setConfiguration(conf);
    try {
//      UserGroupInformation. loginUserFromKeytab("hdfs/bjs0-3a4.big1.lf.wanda.cn@IDC.WANDA-GROUP.NET", "/Users/liyh/Documents/hdfs.keytab" );
      UserGroupInformation. loginUserFromKeytab("exchange_app_user@IDC.WANDA-GROUP.NET", "/Users/liyh/Documents/exchange_app_user.keytab" );
    }catch {
      case e:Exception=>e.printStackTrace()
    }
    val driverName = "org.apache.hive.jdbc.HiveDriver"
    val url = "jdbc:hive2://10.199.192.11:10000/default;principal=hive/bjs0-3a4.big1.lf.wanda.cn@IDC.WANDA-GROUP.NET"
    val user = ""
    val password = ""
    Class.forName(driverName)
    val con = DriverManager.getConnection(url)
    val query = """create table table_name2 (
                  |  id                int,
                  |  dtDontQuery       string,
                  |  name              string
                  |)""".stripMargin
    val stat = con.prepareStatement(query)
    stat.execute()
    stat.close()
    con.close()
    println(query)
  }

}
