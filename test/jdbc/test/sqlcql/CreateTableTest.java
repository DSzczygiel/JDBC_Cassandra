package jdbc.test.sqlcql;

import org.junit.jupiter.api.Test;

import net.sf.jsqlparser.JSQLParserException;
import pl.dszczygiel.jdbc.driver.sqlcql.CreateTableStatement;

public class CreateTableTest {

	@Test
	public void createTableTest() throws JSQLParserException {
		String createTableString = "CREATE TABLE employee (\n" + 
				"    employee_id INT PRIMARY KEY,\n" + 
				"    first_name VARCHAR (50) NOT NULL,\n" + 
				"    last_name VARCHAR (50) NOT NULL,\n" + 
				"    phone VARCHAR(20),\n" + 
				"    department_id INT NOT NULL,\n" + 
				"    FOREIGN KEY (department_id) REFERENCES department (department_id)\n" + 
				");";
		
		CreateTableStatement cts = new CreateTableStatement(createTableString);
		System.out.println(cts);
	}
}
