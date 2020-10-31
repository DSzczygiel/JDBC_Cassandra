package jdbc.test.jdbcwrappers;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import org.junit.jupiter.api.Test;

import pl.dszczygiel.jdbc.driver.CassandraConnection;
import pl.dszczygiel.jdbc.driver.CassandraResultSet;
import pl.dszczygiel.jdbc.driver.CassandraResultSetMetaData;
import pl.dszczygiel.jdbc.driver.CassandraStatement;
import pl.dszczygiel.jdbc.driver.PagingState;
import pl.dszczygiel.jdbc.nativeprotocol.constants.Consistency;

public class WrapperSelectTest {

	@Test
	public void selectTest() {
		try {
			Class.forName("jdbc.driver.CassandraDriver");
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		try {
			Properties properties = new Properties();
			properties.put("username", "cassandra");
			properties.put("password", "cassandra");
			
			CassandraConnection con = (CassandraConnection) DriverManager.getConnection(
					"jdbc:cassandra://127.0.0.1:9042/jdbckeyspace?defaultConsistency=ONE", properties);
			
			CassandraStatement statement = (CassandraStatement) con.createStatement();
			statement.setFetchSize(5);
			statement.setInt("user_id", 2);
			statement.setConsistency(Consistency.LOCAL_ONE);
			CassandraResultSet rs = (CassandraResultSet) statement.executeQuery("SELECT * FROM users WHERE id= :user_id");
			
			CassandraResultSetMetaData meta = (CassandraResultSetMetaData) rs.getMetaData();
			
			
			System.out.println();
			for(int i=0; i<meta.getColumnCount(); i++) {
				System.out.print(meta.getColumnName(i) + ":" + meta.getColumnTypeName(i) + " | ");
			}
			System.out.println("\n");

			while(rs.next()) {
				System.out.println(rs.getInt(0) + " | " +rs.getString("name") + " | " + rs.getInt("age") + " | " + rs.getBigDecimal(2));
			}
			System.out.println("\n");

			rs = (CassandraResultSet) statement.executeQuery("SELECT * FROM users");
			rs.setAutoFetch(false);

			while(rs.next()) {
				System.out.println(rs.getInt(0) + " | " +rs.getString("name") + " | " + rs.getInt("age") + " | " + rs.getBigDecimal(2));
			}
			PagingState state = rs.getPagingState();
			statement.setPagingState(state);
			System.out.println("\n");

			rs = (CassandraResultSet) statement.executeQuery("SELECT * FROM users");
			rs.setAutoFetch(false);
			while(rs.next()) {
				System.out.println(rs.getInt(0) + " | " +rs.getString("name") + " | " + rs.getInt("age") + " | " + rs.getBigDecimal(2));
			}
			
			
			con.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
