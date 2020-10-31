package jdbc.test.sqlcql;

import org.junit.jupiter.api.Test;

import pl.dszczygiel.jdbc.driver.sqlcql.CreateDatabaseStatement;

public class CreateDatabaseTest {
	
	@Test
	public void createDatabaseTest() {
		String createDatabaseString = "CREATE DATABASE testdb";
		
		CreateDatabaseStatement cds = new CreateDatabaseStatement(createDatabaseString);
		System.out.println(cds);
	}
}
