package pl.dszczygiel.jdbc.driver.sqlcql;

import java.util.Map;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.relational.IsNullExpression;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.create.view.CreateView;
import net.sf.jsqlparser.statement.select.PlainSelect;
import pl.dszczygiel.jdbc.system.CassandraPrimaryKey;

public class CreateViewStatement {
	String statement;

	public CreateViewStatement(String statement, CassandraPrimaryKey primaryKey) throws JSQLParserException {
		CreateView cv = (CreateView) CCJSqlParserUtil.parse(statement);
		PlainSelect ps = (PlainSelect) cv.getSelect().getSelectBody();
		IsNullExpression nullEx = new IsNullExpression();
		String partKey = primaryKey.getPartitionKey();

		IsNullExpression partitionKeyNotNullExpression = new IsNullExpression();

		partitionKeyNotNullExpression.setNot(true);
		partitionKeyNotNullExpression.setLeftExpression(new Column(partKey));
		nullEx.setNot(true);
		nullEx.setLeftExpression(new Column(partKey));
		StringBuilder sb = new StringBuilder();
		sb.append(ps.getWhere() + " AND " + partKey + " IS NOT NULL");

		for (Map.Entry<Integer, String> entry : primaryKey.getClusteringKeys().entrySet()) {
			sb.append(" AND " + entry.getValue() + " IS NOT NULL");
		}
		Expression afterNotNulls = CCJSqlParserUtil.parseCondExpression(sb.toString());
		ps.setWhere(afterNotNulls);
		cv.getSelect().setSelectBody(ps);
		sb = new StringBuilder();
		sb.append(" PRIMARY KEY(").append(partKey);
		for (Map.Entry<Integer, String> entry : primaryKey.getClusteringKeys().entrySet()) {
			sb.append(", " + entry.getValue());
		}
		sb.append(")");

		String tmp = cv + sb.toString();
		this.statement = tmp.replaceAll("(?i)CREATE VIEW", "CREATE MATERIALIZED VIEW");

	}

	@Override
	public String toString() {
		return this.statement;
	}
}
