package pl.dszczygiel.jdbc.nativeprotocol.message.responses;

public class RowsResultFlags {
	private boolean globalTablesSpecFlag;
	private boolean hasMorePagesFlag;
	private boolean noMetadataFlag;
	
	public boolean isSetGlobalTablesSpecFlag() {
		return globalTablesSpecFlag;
	}
	public void setGlobalTablesSpecFlag(boolean globalTablesSpecFlag) {
		this.globalTablesSpecFlag = globalTablesSpecFlag;
	}
	public boolean isSetHasMorePagesFlag() {
		return hasMorePagesFlag;
	}
	public void setHasMorePagesFlag(boolean hasMorePagesFlag) {
		this.hasMorePagesFlag = hasMorePagesFlag;
	}
	public boolean isSetNoMetadataFlag() {
		return noMetadataFlag;
	}
	public void setNoMetadataFlag(boolean noMetadataFlag) {
		this.noMetadataFlag = noMetadataFlag;
	}
	
	
}
