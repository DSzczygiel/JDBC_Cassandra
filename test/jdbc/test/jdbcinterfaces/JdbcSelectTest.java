package jdbc.test.jdbcinterfaces;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import org.junit.jupiter.api.Test;

public class JdbcSelectTest {

	@Test
	public void selectTest() {
		try {
			Class.forName("pl.dszczygiel.jdbc.driver.CassandraDriver");
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		try {
			Properties properties = new Properties();
			properties.put("username", "cassandra");
			properties.put("password", "cassandra");
			
			Connection con = DriverManager.getConnection(
					"jdbc:dszczygiel:cassandra://127.0.0.1:9042/testkeyspace?defaultConsistency=ONE", properties);
			
			Statement statement = con.createStatement();
			//statement.setFetchSize(5);
			ResultSet rs = statement.executeQuery("SELECT * FROM sales LIMIT 200");
			ResultSetMetaData meta = rs.getMetaData();
			
			System.out.println();
			for(int i=0; i<meta.getColumnCount(); i++) {
				System.out.print(meta.getColumnName(i) + ":" + meta.getColumnTypeName(i) + " | ");
			}
			System.out.println("\n");

			while(rs.next()) {
				System.out.println(rs.getInt(0) + " | " +rs.getString("name") + " | " + rs.getInt("age") + " | " + rs.getBigDecimal(2));
			}
			
			System.out.println("\n---PREV---");

			while(rs.previous()) {
				System.out.println(rs.getInt(0) + " | " +rs.getString("name") + " | " + rs.getInt("age") + " | " + rs.getBigDecimal(2));
			}
			con.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
