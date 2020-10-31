package pl.dszczygiel.jdbc.driver;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Connection;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;

import org.slf4j.LoggerFactory;

import pl.dszczygiel.jdbc.nativeprotocol.constants.BatchType;
import pl.dszczygiel.jdbc.nativeprotocol.constants.CQLType;
import pl.dszczygiel.jdbc.nativeprotocol.constants.Consistency;
import pl.dszczygiel.jdbc.nativeprotocol.constants.OpCode;
import pl.dszczygiel.jdbc.nativeprotocol.encoders.framelayout.CQLList;
import pl.dszczygiel.jdbc.nativeprotocol.encoders.framelayout.CQLSerializer;
import pl.dszczygiel.jdbc.nativeprotocol.encoders.framelayout.ValueType;
import pl.dszczygiel.jdbc.nativeprotocol.message.responses.ResultKind;
import pl.dszczygiel.jdbc.nativeprotocol.message.responses.ResultMessage;
import pl.dszczygiel.jdbc.nativeprotocol.messages.requests.BatchMessage;
import pl.dszczygiel.jdbc.nativeprotocol.messages.requests.QueryFlags;
import pl.dszczygiel.jdbc.nativeprotocol.messages.requests.QueryMessage;
import pl.dszczygiel.jdbc.nativeprotocol.messages.requests.QueryMessage.QueryMessageType;
import pl.dszczygiel.jdbc.nativeprotocol.types.CQLListSetTypeMetadata;
import pl.dszczygiel.jdbc.nativeprotocol.types.CQLTypeMetadata;
import pl.dszczygiel.jdbc.nativeprotocol.types.UserDefinedType;

public class CassandraStatement implements Statement {
	private static final org.slf4j.Logger logger = LoggerFactory.getLogger(CassandraStatement.class);

	CassandraConnection connection;
	QueryMessage queryMessage;
	BatchMessage batchMessage = new BatchMessage();
	protected CassandraResultSet resultSet;
	int statementTimeOut = 60;
	List<QueryMessage> batchQueries = new ArrayList<>();
	protected Map<String, ValueType> namedValues = new HashMap<String, ValueType>();
	protected Map<Integer, ValueType> values = new TreeMap<Integer, ValueType>();

	CassandraStatement(Connection con) {
		this.connection = (CassandraConnection) con;
		this.queryMessage = new QueryMessage();
		this.queryMessage.setMessageType(QueryMessageType.QUERY);
		this.queryMessage.setConsistency(connection.connectionProperties.getDefaultConsistency());
	}

	CassandraStatement(Connection con, String query) {
		this(con);
		this.queryMessage.setQuery(query);
	}

	@Override
	public boolean isWrapperFor(Class<?> arg0) throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public <T> T unwrap(Class<T> arg0) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void addBatch(String arg0) throws SQLException {
		QueryMessage qm = new QueryMessage();
		qm.setQuery(arg0);
		qm.setMessageType(QueryMessageType.QUERY);
		batchQueries.add(qm);
	}

	@Override
	public void cancel() throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void clearBatch() throws SQLException {
		batchQueries.clear();
	}

	@Override
	public void clearWarnings() throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void close() throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void closeOnCompletion() throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public boolean execute(String query) throws SQLException {
		queryMessage.setQuery(query);
		connection.sendMessage(queryMessage, OpCode.QUERY);
		ResultMessage message = (ResultMessage) connection.getResponse(statementTimeOut).getMessage();
		if (message.getKind() == ResultKind.ROWS) {
			resultSet = new CassandraResultSet(this, message);
			return true;
		}
		return false;
	}

	@Override
	public boolean execute(String arg0, int arg1) throws SQLException {
		throw new SQLFeatureNotSupportedException("Auto generated keys are not supported");
	}

	@Override
	public boolean execute(String arg0, int[] arg1) throws SQLException {
		throw new SQLFeatureNotSupportedException("Auto generated keys are not supported");

	}

	@Override
	public boolean execute(String arg0, String[] arg1) throws SQLException {
		throw new SQLFeatureNotSupportedException("Auto generated keys are not supported");
	}
	
	private ResultMessage runStatement(String query) throws SQLException {
		checkValues();
		queryMessage.setQuery(query);
		connection.sendMessage(queryMessage, OpCode.QUERY);
		ResultMessage message = (ResultMessage) connection.getResponse(statementTimeOut).getMessage();
		return message;
	}

	@Override
	public int[] executeBatch() throws SQLException {
		QueryFlags qf = new QueryFlags();
		Timestamp timestamp = new Timestamp(System.currentTimeMillis());
		qf.setDefaultTimestampFlag(true);
		batchMessage.setBatchType(BatchType.LOGGED);
		batchMessage.setConsistency(Consistency.LOCAL_ONE);
		batchMessage.setQueries(batchQueries);
		batchMessage.setQueriesCount(batchQueries.size());
		batchMessage.setTimestamp(timestamp.getTime());
		batchMessage.setFlags(qf);
		logger.warn("Using default values for batch: Type:LOGGED, Consistency:LOCAL_ONE, Using timestamp:true.");

		connection.sendMessage(batchMessage, OpCode.BATCH);
		connection.getResponse(statementTimeOut);
		logger.warn("executeBatch() returns empty array as Cassandra doesn't return any result for batch operation.");

		return new int[0];
	}

	@Override
	public ResultSet executeQuery(String query) throws SQLException {
		ResultMessage message = runStatement(query);
		resultSet = new CassandraResultSet(this, message);

		return resultSet;
	}

	@Override
	public int executeUpdate(String query) throws SQLException {
			ResultMessage message = runStatement(query);

		if (message.getKind() == ResultKind.ROWS)
			throw new SQLException("executeUpdate() method cannot be used with SELECT statement");
		logger.warn(
				"executeUpdate() always return 0, because Cassandra doesnt return any information about insert/update");
		return 0;
	}

	@Override
	public int executeUpdate(String arg0, int arg1) throws SQLException {
		throw new SQLFeatureNotSupportedException("Auto generated keys are not supported");
	}

	@Override
	public int executeUpdate(String arg0, int[] arg1) throws SQLException {
		throw new SQLFeatureNotSupportedException("Auto generated keys are not supported");
	}

	@Override
	public int executeUpdate(String arg0, String[] arg1) throws SQLException {
		throw new SQLFeatureNotSupportedException("Auto generated keys are not supported");

	}

	@Override
	public Connection getConnection() throws SQLException {
		return this.connection;
	}

	@Override
	public int getFetchDirection() throws SQLException {
		return ResultSet.FETCH_FORWARD;
	}

	@Override
	public int getFetchSize() throws SQLException {
		return queryMessage.getPageSize();
	}

	@Override
	public ResultSet getGeneratedKeys() throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int getMaxFieldSize() throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getMaxRows() throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public boolean getMoreResults() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean getMoreResults(int arg0) throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public int getQueryTimeout() throws SQLException {
		return this.statementTimeOut;
	}

	@Override
	public ResultSet getResultSet() throws SQLException {
		return resultSet;
	}

	@Override
	public int getResultSetConcurrency() throws SQLException {
		return resultSet.getConcurrency();
	}

	@Override
	public int getResultSetHoldability() throws SQLException {
		return resultSet.getHoldability();
	}

	@Override
	public int getResultSetType() throws SQLException {
		return resultSet.getType();
	}

	@Override
	public int getUpdateCount() throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public SQLWarning getWarnings() throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean isCloseOnCompletion() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean isClosed() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean isPoolable() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void setCursorName(String arg0) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void setEscapeProcessing(boolean arg0) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void setFetchDirection(int arg0) throws SQLException {
			// TODO Auto-generated method stub

	}

	@Override
	public void setFetchSize(int arg0) throws SQLException {
		if (arg0 < 0) {
			queryMessage.getQueryFlags().setPageSizeFlag(false);
			logger.info("Disabling pagination.");
		} else if ((arg0 > 0) && (arg0 < 100)) {
			logger.warn("Page size is low. It may affect the performance.");
			queryMessage.getQueryFlags().setPageSizeFlag(true);
			queryMessage.setPageSize(arg0);
		} else if (arg0 == 0) {
			queryMessage.getQueryFlags().setPageSizeFlag(true);
			queryMessage.setPageSize(5000);
			logger.info("Setting page size to default value 5000.");
		} else {
			queryMessage.getQueryFlags().setPageSizeFlag(true);
			queryMessage.setPageSize(arg0);
		}
	}

	@Override
	public void setMaxFieldSize(int arg0) throws SQLException {
		// TODO Auto-generated method stub

	}

	// TODO limit nr of rows
	@Override
	public void setMaxRows(int arg0) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void setPoolable(boolean arg0) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void setQueryTimeout(int arg0) throws SQLException {
		if (arg0 <= 0)
			throw new IllegalArgumentException("Timeout value must be equal or greater than zero.");
		statementTimeOut = arg0;
	}

	
	
	/////////////////////////////////////CUSTOM METHODS
	
	private void checkValues() throws SQLException {
		if(!(values.isEmpty()) && !(namedValues.isEmpty())) {
			throw new SQLException("Cannot use both values and named values. Please choose one");
		}else if(!values.isEmpty()) {
			queryMessage.setValues(values);
			queryMessage.getQueryFlags().setValuesFlag(true);
		}else if(!namedValues.isEmpty()) {
			queryMessage.setNamedValues(namedValues);
			queryMessage.getQueryFlags().setValuesFlag(true);
			queryMessage.getQueryFlags().setNamesForValuesFlag(true);
		}
	}
	
	public void clearStatementValues() {
		queryMessage.getQueryFlags().setNamesForValuesFlag(false);
		queryMessage.getQueryFlags().setValuesFlag(false);
		values.clear();
		namedValues.clear();
	}
	
	public void setConsistency(Consistency consistency) {
		queryMessage.setConsistency(consistency);
	}
	
	public void clearSerialConsistency() {
		queryMessage.getQueryFlags().setSerialConsistencyFlag(false);
		queryMessage.setSerialConsistency(null);
	}
	
	public void setDefaultTimestamp(long timestamp) {
		queryMessage.getQueryFlags().setDefaultTimestampFlag(true);
		queryMessage.setTimestamp(timestamp);
	}
	
	public void setDouble(int index, Double value) {
		values.put(index, new ValueType(new CQLTypeMetadata(CQLType.DOUBLE), value));
	}
	
	public void setDouble(String name, Double value) {
		namedValues.put(name, new ValueType(new CQLTypeMetadata(CQLType.DOUBLE), value));
	}
	
	public void setFloat(int index, Float value) {
		values.put(index, new ValueType(new CQLTypeMetadata(CQLType.FLOAT), value));
	}
	
	public void setFloat(String name, Float value) {
		namedValues.put(name, new ValueType(new CQLTypeMetadata(CQLType.FLOAT), value));
	}
	
	public void setAscii(int index, String value) {
		values.put(index, new ValueType(new CQLTypeMetadata(CQLType.ASCII), value));
	}
	
	public void setAscii(String name, String value) {
		namedValues.put(name, new ValueType(new CQLTypeMetadata(CQLType.ASCII), value));
	}
	
	public void setVarint(int index, BigInteger value) {
		values.put(index, new ValueType(new CQLTypeMetadata(CQLType.VARINT), value));
	}
	
	public void setVarint(String name, BigInteger value) {
		namedValues.put(name, new ValueType(new CQLTypeMetadata(CQLType.VARINT), value));
	}
	
	public void setDecimal(int index, BigDecimal value) {
		values.put(index, new ValueType(new CQLTypeMetadata(CQLType.DECIMAL), value));
	}
	
	public void setDecimal(String name, BigDecimal value) {
		namedValues.put(name, new ValueType(new CQLTypeMetadata(CQLType.DECIMAL), value));	
		}
	
	public void setInt(int index, Integer value) {
		values.put(index, new ValueType(new CQLTypeMetadata(CQLType.INT), value));
	}
	
	public void setInt(String name, Integer value) {
		namedValues.put(name, new ValueType(new CQLTypeMetadata(CQLType.INT), value));
	}
	
	public void setTime(int index, Integer value) {
		values.put(index, new ValueType(new CQLTypeMetadata(CQLType.TIME), value));
	}
	
	public void setTime(String name, Integer value) {
		namedValues.put(name, new ValueType(new CQLTypeMetadata(CQLType.TIME), value));
	}
	
	public void setDate(int index, Date value) {
		values.put(index, new ValueType(new CQLTypeMetadata(CQLType.DATE), value));
	}
	
	public void setDate(String name, Date value) {
		namedValues.put(name, new ValueType(new CQLTypeMetadata(CQLType.DATE), value));
	}
	
	public void setTimestamp(int index, Timestamp value) {
		values.put(index, new ValueType(new CQLTypeMetadata(CQLType.TIMESTAMP), value));
	}
	
	public void setTimestamp(String name, Timestamp value) {
		namedValues.put(name, new ValueType(new CQLTypeMetadata(CQLType.TIMESTAMP), value));
	}
	
	public void setBlob(int index, byte[] value) {
		values.put(index, new ValueType(new CQLTypeMetadata(CQLType.BLOB), value));
	}
	
	public void setBlob(String name, byte[] value) {
		namedValues.put(name, new ValueType(new CQLTypeMetadata(CQLType.BLOB), value));
	}
	
	public void setBoolean(int index, Boolean value) {
		values.put(index, new ValueType(new CQLTypeMetadata(CQLType.BOOLEAN), value));
	}
	
	public void setBoolean(String name, Boolean value) {
		namedValues.put(name, new ValueType(new CQLTypeMetadata(CQLType.BOOLEAN), value));
	}
	
	public void setTinyInt(int index, Byte value) {
		values.put(index, new ValueType(new CQLTypeMetadata(CQLType.TINYINT), value));
	}
	
	public void setTinyInt(String name, Byte value) {
		namedValues.put(name, new ValueType(new CQLTypeMetadata(CQLType.TINYINT), value));
	}
	
	public void setUUID(int index, UUID value) {
		values.put(index, new ValueType(new CQLTypeMetadata(CQLType.UUID), value));
	}
	
	public void setUUID(String name, UUID value) {
		namedValues.put(name, new ValueType(new CQLTypeMetadata(CQLType.UUID), value));
	}
	
	public <T> void setList(int index, List<T> value, Class<T> typeClass) {
		CQLType elementType = CQLSerializer.getCQLTypeForClass(typeClass);
		CQLListSetTypeMetadata meta = new CQLListSetTypeMetadata(CQLType.LIST, elementType);
		values.put(index, new ValueType(meta, value));
	}
	
	public void setUDT(int index, UserDefinedType udt) {
		values.put(index, new ValueType(udt.getMeta(), udt));
	}
	
	public void setSerialConsistency(Consistency consistency) {
		if(consistency == Consistency.SERIAL || consistency == Consistency.LOCAL_SERIAL) {
			queryMessage.setSerialConsistency(consistency);
		}else {
			queryMessage.setSerialConsistency(Consistency.LOCAL_SERIAL);
		}
		queryMessage.getQueryFlags().setSerialConsistencyFlag(true);
	}
	
	public void setText(int index, String value) {
		values.put(index, new ValueType(new CQLTypeMetadata(CQLType.VARCHAR), value));
	}
	
	public void setText(String name, String value) {
		namedValues.put(name, new ValueType(new CQLTypeMetadata(CQLType.VARCHAR), value));
	}
	
	public void setPagingState(PagingState pagingState) {
		this.queryMessage.setPagingState(pagingState);
		if(!pagingState.isEmpty()) {
			this.queryMessage.getQueryFlags().setPagingStateFlag(true);
		}
	}

	public void clearPagingState() {
		this.queryMessage.setPagingState(new PagingState(null));
		this.queryMessage.getQueryFlags().setPagingStateFlag(false);
	}
	
}
