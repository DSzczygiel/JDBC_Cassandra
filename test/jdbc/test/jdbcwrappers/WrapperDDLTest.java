package jdbc.test.jdbcwrappers;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import org.junit.jupiter.api.Test;

import pl.dszczygiel.jdbc.driver.CassandraConnection;
import pl.dszczygiel.jdbc.driver.CassandraStatement;
import pl.dszczygiel.jdbc.driver.SchemaChangeListener;
import pl.dszczygiel.jdbc.nativeprotocol.message.responses.SchemaChangeData;

public class WrapperDDLTest {
	
	@Test
	public void ddlTest() {
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
			
			con.setSchemaChangeListener(new SchemaChangeListener() {	
				@Override
				public void onSchemaChange(SchemaChangeData schemaChangeData) {
					System.out.println("Schema change event");
					System.out.println("Keyspace " + schemaChangeData.getAffectedKeyspace() + 
							", Table " + schemaChangeData.getAffectedTable() +
							", Operation: " + schemaChangeData.getChangeType());
				}
			});
			
			CassandraStatement statement = (CassandraStatement) con.createStatement();
			statement.execute("CREATE TABLE IF NOT EXISTS testtable (id int PRIMARY KEY)");
			statement.execute("DROP TABLE IF EXISTS testtable");

			con.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
