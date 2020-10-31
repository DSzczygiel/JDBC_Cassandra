package pl.dszczygiel.jdbc.nativeprotocol.messages.requests;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import pl.dszczygiel.jdbc.driver.PagingState;
import pl.dszczygiel.jdbc.nativeprotocol.constants.Consistency;
import pl.dszczygiel.jdbc.nativeprotocol.encoders.framelayout.ValueType;
import pl.dszczygiel.jdbc.nativeprotocol.frame.Message;

public class QueryMessage extends Message{
	private QueryMessageType messageType;
	private byte[] queryId;
	private String query;
	private QueryFlags queryFlags;
	private Map<String, ValueType> namedValues;
	private Map<Integer, ValueType> values;
	private int pageSize;
	private PagingState pagingState;
	private Consistency consistency;
	private long timestamp;
	private Consistency serialConsistency;
	
	public QueryMessage() {
		namedValues = new HashMap<String, ValueType>();
		values = new TreeMap<Integer, ValueType>();
		queryFlags = new QueryFlags();
		queryFlags.setPageSizeFlag(true);
		pageSize = 5000;
	}
	
	public enum QueryMessageType {
		QUERY, EXECUTE;
	}
	
	public QueryMessageType getMessageType() {
		return messageType;
	}
	public void setMessageType(QueryMessageType messageType) {
		this.messageType = messageType;
	}
	public byte[] getQueryId() {
		return queryId;
	}
	public void setQueryId(byte[] bs) {
		this.queryId = bs;
	}
	public String getQuery() {
		return query;
	}
	public void setQuery(String query) {
		this.query = query;
	}
	public QueryFlags getQueryFlags() {
		return queryFlags;
	}
	public void setQueryFlags(QueryFlags queryFlags) {
		this.queryFlags = queryFlags;
	}
	public int getPageSize() {
		return pageSize;
	}
	public void setPageSize(int pageSize) {
		this.pageSize = pageSize;
	}
	public PagingState getPagingState() {
		return pagingState;
	}
	public void setPagingState(PagingState pagingState) {
		this.pagingState = pagingState;
	}
	public Consistency getConsistency() {
		return consistency;
	}
	public void setConsistency(Consistency consistency) {
		this.consistency = consistency;
	}
	public long getTimestamp() {
		return timestamp;
	}
	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}
	public Consistency getSerialConsistency() {
		return serialConsistency;
	}
	public void setSerialConsistency(Consistency serialConsistency) {
		this.serialConsistency = serialConsistency;
	}

	public Map<String, ValueType> getNamedValues() {
		return namedValues;
	}

	public void setNamedValues(Map<String, ValueType> namedValues) {
		this.namedValues = namedValues;
	}

	public Map<Integer, ValueType> getValues() {
		return values;
	}

	public void setValues(Map<Integer, ValueType> values) {
		this.values = values;
	}
	
	
	
}
