package pl.dszczygiel.jdbc.driver;

import java.util.ArrayList;
import java.util.List;

import pl.dszczygiel.jdbc.nativeprotocol.constants.BatchType;
import pl.dszczygiel.jdbc.nativeprotocol.constants.Consistency;
import pl.dszczygiel.jdbc.nativeprotocol.messages.requests.BatchMessage;
import pl.dszczygiel.jdbc.nativeprotocol.messages.requests.QueryFlags;
import pl.dszczygiel.jdbc.nativeprotocol.messages.requests.QueryMessage;

public class BatchStatement {
	QueryFlags flags = new QueryFlags();
	List<QueryMessage> statements = new ArrayList<QueryMessage>();
	private BatchType batchType = BatchType.LOGGED;
	private Consistency consistency = Consistency.LOCAL_ONE;
	private Consistency serialConsistency = Consistency.LOCAL_SERIAL;
	private long timestamp;
	
	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
		this.flags.setDefaultTimestampFlag(true);
	}
	
	public void clearTimestamp() {
		this.timestamp = 0;
		this.flags.setDefaultTimestampFlag(false);
	}
	
	public void addStatement(CassandraStatement statement) {
		for(QueryMessage qm : statement.batchQueries)
			statements.add(qm);
	}
	
	public void clear() {
		statements.clear();
	}
	
	public void setBatchType(BatchType type) {
		this.batchType = type;;
	}
	
	public void setConsistency(Consistency consistency) {
		this.consistency = consistency;
	}
	
	public void setSerialConsistency(Consistency consistency) {
		this.serialConsistency = consistency;
		flags.setSerialConsistencyFlag(true);
	}
	
	public void clearSerialConsistency() {
		this.flags.setSerialConsistencyFlag(false);
	}

	List<QueryMessage> getStatements() {
		return statements;
	}

	BatchType getBatchType() {
		return batchType;
	}

	Consistency getConsistency() {
		return consistency;
	}

	Consistency getSerialConsistency() {
		return serialConsistency;
	}

	QueryFlags getFlags() {
		return flags;
	}

	long getTimestamp() {
		return timestamp;
	}
}
