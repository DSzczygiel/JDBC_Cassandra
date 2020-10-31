package pl.dszczygiel.jdbc.nativeprotocol.message.responses;

import java.util.List;

import pl.dszczygiel.jdbc.driver.PagingState;
import pl.dszczygiel.jdbc.nativeprotocol.frame.Message;

public class ResultMessage extends Message{
	private RowsResultFlags rowResultFlags;
	private ResultKind kind;
	private PagingState pagingState;
	private int columnCount;
	private List<ColumnSpecification> columnSpecifications;
	private List<Row> rows;
	private PreparedStatementData preparedStatementData;
	
	
	public RowsResultFlags getRowResultFlags() {
		return rowResultFlags;
	}
	public void setRowResultFlags(RowsResultFlags rowResultFlags) {
		this.rowResultFlags = rowResultFlags;
	}
	public ResultKind getKind() {
		return kind;
	}
	public void setKind(ResultKind kind) {
		this.kind = kind;
	}
	public PagingState getPagingState() {
		return pagingState;
	}
	public void setPagingState(PagingState pagingState) {
		this.pagingState = pagingState;
	}
	public int getColumnCount() {
		return columnCount;
	}
	public void setColumnCount(int columnCount) {
		this.columnCount = columnCount;
	}
	public List<ColumnSpecification> getColumnSpecifications() {
		return columnSpecifications;
	}
	public void setColumnSpecifications(List<ColumnSpecification> columnSpecifications) {
		this.columnSpecifications = columnSpecifications;
	}
	public List<Row> getRows() {
		return rows;
	}
	public void setRows(List<Row> rows) {
		this.rows = rows;
	}
	public PreparedStatementData getPreparedStatementData() {
		return preparedStatementData;
	}
	public void setPreparedStatementData(PreparedStatementData preparedStatementData) {
		this.preparedStatementData = preparedStatementData;
	}
	
}
