package pl.dszczygiel.jdbc.driver.sqlcql;

import java.util.List;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;

public class CreateTableStatement {
	private String statement = null;
	public CreateTableStatement(String statement) throws JSQLParserException {
		CreateTable ct = (CreateTable) CCJSqlParserUtil.parse(statement);
		ct.getCreateOptionsStrings();
		ct.getIndexes().clear();
		List<ColumnDefinition> cds = ct.getColumnDefinitions();
		
		for(ColumnDefinition cd : cds) {
			if(cd.getColDataType().getDataType().equalsIgnoreCase("VARCHAR"))
				cd.getColDataType().setArgumentsStringList(null);
		}
		String stat = ct.toString();
		this.statement = stat.replaceAll("(?i) NOT NULL", "");
	}
	
	@Override
	public String toString() {
		return statement;
	}
}
