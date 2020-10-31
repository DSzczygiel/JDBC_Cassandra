package pl.dszczygiel.jdbc.nativeprotocol.message.responses;

public class SchemaChangeData {
	public static final String CHANGE_CREATED = "CREATED";
	public static final String CHANGE_UPDATED = "UPDATED";
	public static final String CHANGE_DROPPED = "DROPPED";

	public static final String TARGET_KEYSPACE = "KEYSPACE";
	public static final String TARGET_TABLE = "TABLE";
	public static final String TARGET_TYPE = "TYPE";
	public static final String TARGET_FUNCTION = "FUNCTION";
	public static final String TARGET_AGGREGATE = "AGGREGATE";

	private String changeType;
	private String target;
	private String affectedKeyspace;
	private String affectedTable;
	
	public String getChangeType() {
		return changeType;
	}
	public void setChangeType(String changeType) {
		this.changeType = changeType;
	}
	public String getTarget() {
		return target;
	}
	public void setTarget(String target) {
		this.target = target;
	}
	public String getAffectedKeyspace() {
		return affectedKeyspace;
	}
	public void setAffectedKeyspace(String affectedKeyspace) {
		this.affectedKeyspace = affectedKeyspace;
	}
	public String getAffectedTable() {
		return affectedTable;
	}
	public void setAffectedTable(String affectedTable) {
		this.affectedTable = affectedTable;
	}
	
	
	
}
