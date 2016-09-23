import java.sql.SQLException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.DriverManager;

public class HiveJdbcClient {
    private static String driverName = "org.apache.hive.jdbc.HiveDriver";
    private Connection _conn = null;
    public void connect(String ip, int port) {
	try {
            Class.forName(HiveJdbcClient.driverName);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            System.exit(1);
        }
        _conn = DriverManager.getConnection("jdbc:hive2://"+ip+":"+String.valueOf(port)+"/default", "", "");	
    }

    public void getTables() throws SQLException {
        Statement stmt = _conn.createStatement();
        String sql = "show tables"; 
        res = stmt.executeQuery(sql);
        if (res.next()) {
            String tables = res.getString(1);
        }
	return tables;
    }

    public void getFieldsForTable(String tableName) throws SQLException {
        Statement stmt = _conn.createStatement();
        String sql = "describe "+ tableName; 
        res = stmt.executeQuery(sql);
	List<String> fields= new ArrayList<String>();
        while (res.next()) {
            fields.add(res.getString(1) + "\t" + res.getString(2));
        }
	return fields;
    }
}
