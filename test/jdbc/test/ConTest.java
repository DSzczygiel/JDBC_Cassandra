package jdbc.test;

import java.sql.DriverManager;
import java.util.Properties;

import org.junit.jupiter.api.Test;

import pl.dszczygiel.jdbc.driver.CassandraConnection;

public class ConTest {
	@Test
	public void contest() {
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

		} catch (Exception e) {
			// TODO: handle exception
		}
	}
}
