package pl.dszczygiel.jdbc.nativeprotocol.message.responses;

import java.util.List;

import pl.dszczygiel.jdbc.driver.PagingState;

public class RowsResult {
	private ResultKind kind;
	private PagingState pagingState;
	private int columnCount;
	private List<ColumnSpecification> columnSpecifications;
	private List<Row> rows;
	
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
	
	
}
