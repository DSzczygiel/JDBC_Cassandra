package pl.dszczygiel.jdbc.nativeprotocol.messages.requests;

public class QueryFlags {
	private boolean valuesFlag;
	private boolean skipMetadataFlag;
	private boolean pageSizeFlag;
	private boolean pagingStateFlag;
	private boolean serialConsistencyFlag;
	private boolean defaultTimestampFlag;
	private boolean namesForValuesFlag;

	public boolean getValuesFlag() {
		return valuesFlag;
	}

	public void setValuesFlag(boolean valuesFlag) {
		this.valuesFlag = valuesFlag;
	}

	public boolean getSkipMetadataFlag() {
		return skipMetadataFlag;
	}

	public void setSkipMetadataFlag(boolean skipMetadataFlag) {
		this.skipMetadataFlag = skipMetadataFlag;
	}

	public boolean getPageSizeFlag() {
		return pageSizeFlag;
	}

	public void setPageSizeFlag(boolean pageSizeFlag) {
		this.pageSizeFlag = pageSizeFlag;
	}

	public boolean getPagingStateFlag() {
		return pagingStateFlag;
	}

	public void setPagingStateFlag(boolean pagingStateFlag) {
		this.pagingStateFlag = pagingStateFlag;
	}

	public boolean getSerialConsistencyFlag() {
		return serialConsistencyFlag;
	}

	public void setSerialConsistencyFlag(boolean serialConsistencyFlag) {
		this.serialConsistencyFlag = serialConsistencyFlag;
	}

	public boolean getDefaultTimestampFlag() {
		return defaultTimestampFlag;
	}

	public void setDefaultTimestampFlag(boolean defaultTimestampFlag) {
		this.defaultTimestampFlag = defaultTimestampFlag;
	}

	public boolean getNamesForValuesFlag() {
		return namesForValuesFlag;
	}

	public void setNamesForValuesFlag(boolean namesForValuesFlag) {
		this.namesForValuesFlag = namesForValuesFlag;
	}

}
