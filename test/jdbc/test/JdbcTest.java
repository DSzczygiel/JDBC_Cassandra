package jdbc.test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.jupiter.api.Test;

import pl.dszczygiel.jdbc.driver.CassandraConnection;
import pl.dszczygiel.jdbc.driver.CassandraStatement;
import pl.dszczygiel.jdbc.driver.SchemaChangeListener;
import pl.dszczygiel.jdbc.nativeprotocol.message.responses.SchemaChangeData;

public class JdbcTest {

	public void selectTest() {
		try {
			Class.forName("jdbc.driver.CassandraDriver");
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		//Connection con = DriverManager.getConnection(
			//	"jdbc:cassandra://127.0.0.1:9042/testkeyspace?primarydc=DC1&backupdc=DC2&consistency=QUORUM");
		try {
			Connection con = DriverManager.getConnection(
					"jdbc:cassandra://127.0.0.1:9042");
			
			Statement statement = con.createStatement();
		//	ResultSet rs = statement.executeQuery("select * from user");
			
		//	while(rs.next()) {
				
				//System.out.println(rs.getInt(0) + " " +rs.getString("name"));
			//}
			
			con.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	//@Test
	public void fetchSizeTest() {
		try {
			Class.forName("jdbc.driver.CassandraDriver");
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		//Connection con = DriverManager.getConnection(
			//	"jdbc:cassandra://127.0.0.1:9042/testkeyspace?primarydc=DC1&backupdc=DC2&consistency=QUORUM");
		try {
			Connection con = DriverManager.getConnection(
					"jdbc:cassandra://127.0.0.1:9042");
			
			Statement statement = con.createStatement();
			statement.setFetchDirection(ResultSet.FETCH_REVERSE);
			statement.setFetchSize(5);
			ResultSet rs = statement.executeQuery("select * from user");
			while(rs.next()) {
				System.out.println(rs.getInt(0) + " :: " +rs.getString("name"));
			}

			con.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	@Test
	public void DCLTest() {
		try {
			Class.forName("jdbc.driver.CassandraDriver");
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		//Connection con = DriverManager.getConnection(
			//	"jdbc:cassandra://127.0.0.1:9042/testkeyspace?primarydc=DC1&backupdc=DC2&consistency=QUORUM");
		try {
			Connection con = DriverManager.getConnection(
					"jdbc:cassandra://127.0.0.1:9042");
			
			Statement statement = con.createStatement();
			((CassandraConnection) con).schemaChangeListener = new SchemaChangeListener() {
				
				@Override
				public void onSchemaChange(SchemaChangeData schemaChangeData) {
					System.out.println("listtner");
					System.out.println(schemaChangeData.getAffectedKeyspace());
					System.out.println(schemaChangeData.getAffectedTable());
					
				}
			};
			statement.execute("drop table if exists f");

			statement.execute("create table if not exists f(id int primary key, name text)");
			

			con.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
