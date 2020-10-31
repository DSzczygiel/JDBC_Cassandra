package jdbc.test.jdbcwrappers;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.junit.jupiter.api.Test;

import pl.dszczygiel.jdbc.driver.CassandraConnection;
import pl.dszczygiel.jdbc.driver.CassandraDatabaseMetaData;
import pl.dszczygiel.jdbc.driver.CassandraPreparedStatement;
import pl.dszczygiel.jdbc.driver.CassandraResultSet;
import pl.dszczygiel.jdbc.driver.CassandraStatement;
import pl.dszczygiel.jdbc.nativeprotocol.types.UserDefinedType;

public class WrapperPreparedTest {

	@Test
	public void prepaerdTest() throws SQLException {
		try {
			Class.forName("jdbc.driver.CassandraDriver");
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}

		Properties properties = new Properties();
		properties.put("username", "cassandra");
		properties.put("password", "cassandra");
		
		CassandraConnection con = (CassandraConnection) DriverManager.getConnection(
				"jdbc:cassandra://127.0.0.1:9042/jdbckeyspace", properties);
		CassandraDatabaseMetaData metadata = (CassandraDatabaseMetaData) con.getMetaData();
		
		CassandraStatement statement = (CassandraStatement) con.createStatement();
		statement.execute("TRUNCATE TABLE visited_cities");
		statement.execute("TRUNCATE TABLE udt_test");
		
		CassandraPreparedStatement preparedStatement = (CassandraPreparedStatement) 
				con.prepareStatement("INSERT INTO visited_cities (user_id, cities) VALUES(?, ?)");
		
		List<String> cities = new ArrayList<>();
		cities.add("Warszawa");
		cities.add("Kielce");
		cities.add("Kraków");
		cities.add("Poznań");
		preparedStatement.setInt(0, 1);
		preparedStatement.setList(1, cities, String.class);
		preparedStatement.execute();
		
		cities.clear();
		cities.add("Kraków");
		cities.add("Warszawa");

		preparedStatement.setInt(0, 2);
		preparedStatement.setList(1, cities, String.class);
		preparedStatement.execute();
		
		CassandraPreparedStatement preparedStatement2 = (CassandraPreparedStatement) 
				con.prepareStatement("INSERT INTO udt_test (user_id, user_info) VALUES(?, ?)");
		
		UserDefinedType udt = metadata.getUdtByName("jdbckeyspace", "user_info");
		udt.setString("gender", "female");
		udt.setInt("age", 22);
		udt.setInt("height", 170);
		
		UserDefinedType udt2 = metadata.getUdtByName("jdbckeyspace", "user_info");
		udt2.setString("gender", "male");
		udt2.setInt("age", 32);
		udt2.setInt("height", 180);
		
		preparedStatement2.setInt(0, 1);
		preparedStatement2.setUDT(1, udt);
		preparedStatement2.execute();
		
		preparedStatement2.setInt(0, 2);
		preparedStatement2.setUDT(1, udt2);
		preparedStatement2.execute();
		
		CassandraResultSet rs = (CassandraResultSet) statement.executeQuery("SELECT * FROM visited_cities");
		while(rs.next()) {
			System.out.print(rs.getInt(0) + " : { ");
			for(String s : rs.getList(1, String.class)) {
				System.out.print(s + " ");
			}
			System.out.println(" }");
		}
		System.out.println();
		rs = (CassandraResultSet) statement.executeQuery("SELECT * FROM udt_test");
		while(rs.next()) {
			UserDefinedType u = rs.getUDT(1);
			System.out.print(rs.getInt(0) + " : ");
			System.out.println("age: " + u.getInt("age") + ", height: " + u.getInt("height") + ". gender: " + u.getString("gender"));
		}
	}
}
