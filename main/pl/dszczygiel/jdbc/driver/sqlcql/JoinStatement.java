package pl.dszczygiel.jdbc.driver.sqlcql;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.commons.collections4.MultiValuedMap;
import org.apache.commons.collections4.multimap.HashSetValuedHashMap;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.ExpressionVisitorAdapter;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.util.TablesNamesFinder;

public class JoinStatement {
	MultiValuedMap<String, String> tablesColumns = new HashSetValuedHashMap<>();
	MultiValuedMap<String, String> joinTablesColumn = new HashSetValuedHashMap<>();
	List<String> statements = new ArrayList<>();
	JoinType joinType;

	enum JoinType {
		INNER, LEFT, RIGHT, OUTER;
	}

	public JoinStatement(String statement) throws JSQLParserException {
		TablesNamesFinder tablesNamesFinder = new TablesNamesFinder();
		Select select = (Select) CCJSqlParserUtil.parse(statement);
		List<String> tableNames = tablesNamesFinder.getTableList(select);
		PlainSelect ps = (PlainSelect) select.getSelectBody();
		List<SelectItem> si = ps.getSelectItems();
		for (SelectItem item : si) {
			String[] tmp = item.toString().split("\\.");
			tablesColumns.put(tmp[0], tmp[1]);
		}

		List<Join> joins = ps.getJoins();
		for (Join j : joins) {
			if (j.isInner())
				joinType = JoinType.INNER;
			else if (j.isLeft())
				joinType = JoinType.LEFT;
			else if (j.isRight())
				joinType = JoinType.RIGHT;
			else if (j.isOuter())
				joinType = JoinType.OUTER;
			j.getOnExpression().accept(new ExpressionVisitorAdapter() {

				@Override
				protected void visitBinaryExpression(BinaryExpression expr) {
					String[] tmp = expr.getLeftExpression().toString().split("\\.");
					tablesColumns.put(tmp[0], tmp[1]);
					joinTablesColumn.put(tmp[0], tmp[1]);
					tmp = expr.getRightExpression().toString().split("\\.");
					tablesColumns.put(tmp[0], tmp[1]);
					joinTablesColumn.put(tmp[0], tmp[1]);
				}

			});

			for (String tab : tableNames) {
				PlainSelect sel = new PlainSelect();
				Set<String> cols = (Set<String>) tablesColumns.get(tab);
				for (String c : cols)
					sel.addSelectItems(new SelectExpressionItem(new Column(c)));

				sel.setFromItem(new Table(tab));
				statements.add(sel.toString());
			}
		}
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("joinColumns: 	").append(joinTablesColumn);
		sb.append("\nstatements: \n");
		for (String s : statements)
			sb.append(s).append("\n");
		sb.append("joinType: ").append(joinType);

		return sb.toString();
	}

}
