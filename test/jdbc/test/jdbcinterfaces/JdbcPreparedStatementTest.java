package jdbc.test.jdbcinterfaces;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import org.junit.jupiter.api.Test;

import pl.dszczygiel.jdbc.driver.CassandraStatement;

public class JdbcPreparedStatementTest {
	@Test
	public void preparedSelectTest() {
		try {
			Class.forName("jdbc.driver.CassandraDriver");
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		try {
			Properties properties = new Properties();
			properties.put("username", "cassandra");
			properties.put("password", "cassandra");
			
			Connection con = DriverManager.getConnection(
					"jdbc:cassandra://127.0.0.1:9042/jdbckeyspace", properties);
			
			Statement statement = con.createStatement();
			statement.execute("TRUNCATE TABLE events");
			ResultSet rs = statement.executeQuery("SELECT * FROM events");
			
			System.out.println("Before prepare");
			while(rs.next()) {
				System.out.println(rs.getInt(0) + " " +rs.getString("name"));
			}
			
			PreparedStatement preparedStatement = con.prepareStatement("INSERT INTO events (id, name) VALUES(?, ?)");
			preparedStatement.setInt(0, 1);
			preparedStatement.setString(1, "Event1");
			preparedStatement.execute();
			
			rs = statement.executeQuery("SELECT * FROM EVENTS");
			System.out.println("After prepare");
			while(rs.next()) {
				System.out.println(rs.getInt(0) + " " +rs.getString("name"));
			}
			
			con.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
