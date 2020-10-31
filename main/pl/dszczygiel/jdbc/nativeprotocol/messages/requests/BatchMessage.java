package pl.dszczygiel.jdbc.nativeprotocol.messages.requests;

import java.util.List;

import pl.dszczygiel.jdbc.nativeprotocol.constants.BatchType;
import pl.dszczygiel.jdbc.nativeprotocol.constants.Consistency;
import pl.dszczygiel.jdbc.nativeprotocol.frame.Message;

public class BatchMessage extends Message{
	private BatchType batchType;
	private int queriesCount;
	private QueryFlags flags;
	private List<QueryMessage> queries;
	private Consistency consistency;
	private Consistency serialConsistency;
	private long timestamp;
	
	public BatchType getBatchType() {
		return batchType;
	}
	public void setBatchType(BatchType batchType) {
		this.batchType = batchType;
	}
	public int getQueriesCount() {
		return queriesCount;
	}
	public void setQueriesCount(int queriesCount) {
		this.queriesCount = queriesCount;
	}
	public QueryFlags getFlags() {
		return flags;
	}
	public void setFlags(QueryFlags flags) {
		this.flags = flags;
	}
	public List<QueryMessage> getQueries() {
		return queries;
	}
	public void setQueries(List<QueryMessage> queries) {
		this.queries = queries;
	}
	public Consistency getConsistency() {
		return consistency;
	}
	public void setConsistency(Consistency consistency) {
		this.consistency = consistency;
	}
	public Consistency getSerialConsistency() {
		return serialConsistency;
	}
	public void setSerialConsistency(Consistency serialConsistency) {
		this.serialConsistency = serialConsistency;
	}
	public long getTimestamp() {
		return timestamp;
	}
	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}
	
	
}
