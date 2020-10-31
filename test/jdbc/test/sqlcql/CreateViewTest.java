package jdbc.test.sqlcql;

import java.util.TreeMap;

import org.junit.jupiter.api.Test;

import net.sf.jsqlparser.JSQLParserException;
import pl.dszczygiel.jdbc.driver.sqlcql.CreateViewStatement;
import pl.dszczygiel.jdbc.system.CassandraPrimaryKey;

public class CreateViewTest {

	@Test
	public void createViewTest() throws JSQLParserException {
		
		CassandraPrimaryKey pk = new CassandraPrimaryKey();
		TreeMap<Integer, String> clusteringKeys = new TreeMap<Integer, String>();
		clusteringKeys.put(0, "production_year");
		pk.setPartitionKey("vin");
		pk.setClusteringKeys(clusteringKeys);
		
		String createViewString = "CREATE VIEW car_by_year_view "
				+ "AS SELECT vin, production_year, make "
				+ "FROM cars WHERE production_year=2013";
		
		CreateViewStatement cvs = new CreateViewStatement(createViewString, pk); 
		
		System.out.println(cvs);
	}
}
