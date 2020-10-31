package jdbc.test.jdbcinterfaces;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.jupiter.api.Test;

public class JdbcBatchTest {

	@Test
	public void batchTest() {
		try {
			Class.forName("jdbc.driver.CassandraDriver");
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		try {
			Connection con = DriverManager.getConnection(
					"jdbc:cassandra://127.0.0.1:9042/jdbckeyspace?username=cassandra&password=cassandra");

			Statement statement = con.createStatement();
			
			statement.execute("TRUNCATE TABLE events");
			ResultSet rs = statement.executeQuery("SELECT * FROM events");
			
			System.out.println("Before batch");
			while(rs.next()) {
				System.out.println(rs.getInt(0) + " | " +rs.getString("name"));
			}
				
			statement.addBatch("INSERT INTO events (id, name) VALUES (1, 'Event 1')");
			statement.addBatch("INSERT INTO events (id, name) VALUES (2, 'Event 2')");
			statement.addBatch("INSERT INTO events (id, name) VALUES (3, 'Event 3')");
			
			statement.executeBatch();
			
			PreparedStatement preparedStatement = con.prepareStatement("INSERT INTO events (id, name) VALUES(?, ?)");
			preparedStatement.setInt(0, 9);
			preparedStatement.setString(1, "Event 9");
			preparedStatement.addBatch();
			
			preparedStatement.setInt(0, 10);
			preparedStatement.setString(1, "Event 10");
			preparedStatement.addBatch();
			
			preparedStatement.executeBatch();
			
			rs = statement.executeQuery("SELECT * FROM events");
			
			System.out.println("After batch");
			while(rs.next()) {
				System.out.println(rs.getInt(0) + " | " +rs.getString("name"));
			}
			
			con.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
