package pl.dszczygiel.jdbc.nativeprotocol.message.responses;

import java.util.List;

public class PreparedStatementData {
	private byte[] id;
	private int columnCount;
	private int primaryKeyCount;
	private List<Integer> primaryKeysIndexes;
	private List<ColumnSpecification> columnSpecifications;
	
	public byte[] getId() {
		return id;
	}
	public void setId(byte[] id) {
		this.id = id;
	}
	public int getColumnCount() {
		return columnCount;
	}
	public void setColumnCount(int columnCount) {
		this.columnCount = columnCount;
	}
	public int getPrimaryKeyCount() {
		return primaryKeyCount;
	}
	public void setPrimaryKeyCount(int primaryKeyCount) {
		this.primaryKeyCount = primaryKeyCount;
	}
	public List<ColumnSpecification> getColumnSpecifications() {
		return columnSpecifications;
	}
	public void setColumnSpecifications(List<ColumnSpecification> columnSpecifications) {
		this.columnSpecifications = columnSpecifications;
	}
	public List<Integer> getPrimaryKeysIndexes() {
		return primaryKeysIndexes;
	}
	public void setPrimaryKeysIndexes(List<Integer> primaryKeysIndexes) {
		this.primaryKeysIndexes = primaryKeysIndexes;
	}
	
	
}
