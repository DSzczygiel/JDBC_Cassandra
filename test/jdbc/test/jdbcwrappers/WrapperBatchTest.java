package jdbc.test.jdbcwrappers;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import org.junit.jupiter.api.Test;

import pl.dszczygiel.jdbc.driver.BatchStatement;
import pl.dszczygiel.jdbc.driver.CassandraConnection;
import pl.dszczygiel.jdbc.driver.CassandraPreparedStatement;
import pl.dszczygiel.jdbc.driver.CassandraResultSet;
import pl.dszczygiel.jdbc.driver.CassandraStatement;

public class WrapperBatchTest {
	@Test
	public void batchTest() {
		try {
			Class.forName("jdbc.driver.CassandraDriver");
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		try {
			Properties properties = new Properties();
			properties.put("username", "cassandra");
			properties.put("password", "cassandra");

			CassandraConnection con = (CassandraConnection) DriverManager
					.getConnection("jdbc:cassandra://127.0.0.1:9042/jdbckeyspace?defaultConsistency=ONE", properties);

			BatchStatement batchStatement = new BatchStatement();

			CassandraStatement statement = (CassandraStatement) con.createStatement();

			statement.execute("TRUNCATE TABLE events");
			CassandraResultSet rs = (CassandraResultSet) statement.executeQuery("SELECT * FROM events");

			System.out.println("Before batch");
			while (rs.next()) {
				System.out.println(rs.getInt(0) + " | " + rs.getString("name"));
			}

			statement.addBatch("INSERT INTO events (id, name) VALUES (1, 'Event 1')");
			statement.addBatch("INSERT INTO events (id, name) VALUES (2, 'Event 2')");
			statement.addBatch("INSERT INTO events (id, name) VALUES (3, 'Event 3')");

			CassandraPreparedStatement preparedStatement = (CassandraPreparedStatement) con
					.prepareStatement("INSERT INTO events (id, name) VALUES(?, ?)");
			preparedStatement.setInt(0, 9);
			preparedStatement.setString(1, "Event 9");
			preparedStatement.addBatch();

			preparedStatement.setInt(0, 10);
			preparedStatement.setString(1, "Event 10");
			preparedStatement.addBatch();

			batchStatement.addStatement(statement);
			batchStatement.addStatement(preparedStatement);
			
			con.runBatch(batchStatement);
			
			rs = (CassandraResultSet) statement.executeQuery("SELECT * FROM events");

			System.out.println("After batch");
			while (rs.next()) {
				System.out.println(rs.getInt(0) + " | " + rs.getString("name"));
			}

			con.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
