package pl.dszczygiel.jdbc.driver;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectOutputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Map;
import java.util.TreeMap;

import javax.sql.rowset.serial.SerialBlob;

import org.slf4j.LoggerFactory;

import pl.dszczygiel.jdbc.driver.exceptions.CassandraException;
import pl.dszczygiel.jdbc.driver.exceptions.UnpreparedException;
import pl.dszczygiel.jdbc.nativeprotocol.constants.CQLType;
import pl.dszczygiel.jdbc.nativeprotocol.constants.OpCode;
import pl.dszczygiel.jdbc.nativeprotocol.encoders.framelayout.ValueType;
import pl.dszczygiel.jdbc.nativeprotocol.message.responses.ResultKind;
import pl.dszczygiel.jdbc.nativeprotocol.message.responses.ResultMessage;
import pl.dszczygiel.jdbc.nativeprotocol.messages.requests.PrepareMessage;
import pl.dszczygiel.jdbc.nativeprotocol.messages.requests.QueryMessage;
import pl.dszczygiel.jdbc.nativeprotocol.messages.requests.QueryMessage.QueryMessageType;
import pl.dszczygiel.jdbc.nativeprotocol.types.CQLTypeMetadata;

public class CassandraPreparedStatement extends CassandraStatement implements PreparedStatement {
	private static final org.slf4j.Logger logger = LoggerFactory.getLogger(CassandraPreparedStatement.class);

	//private Map<Integer, ValueType> values = new TreeMap<Integer, ValueType>();
	ResultMessage preparedResultMessage;

	CassandraPreparedStatement(CassandraConnection connection, String query) {
		super(connection);
		queryMessage.setQuery(query);
		queryMessage.getQueryFlags().setValuesFlag(true);
		queryMessage.setMessageType(QueryMessageType.EXECUTE);
		connection.sendMessage(new PrepareMessage(query), OpCode.PREPARE);
		preparedResultMessage = (ResultMessage) connection.getResponse(statementTimeOut).getMessage();
	}

	protected void reprepareQuery() {
		connection.sendMessage(new PrepareMessage(queryMessage.getQuery()), OpCode.PREPARE);
		preparedResultMessage = (ResultMessage) connection.getResponse(statementTimeOut).getMessage();
	}

	protected ResultMessage runExecute() {
		int retries = 2;
		while (true) {
			try {
				//if(retries == 2)
					//queryMessage.setQueryId(new byte[] {0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 0, 1, 0 ,0,});
				//else
					queryMessage.setQueryId(preparedResultMessage.getPreparedStatementData().getId());

				//queryMessage.setQueryId(preparedResultMessage.getPreparedStatementData().getId());
				queryMessage.setValues(values);
				connection.sendMessage(queryMessage, OpCode.EXECUTE);
				ResultMessage resultMessage = (ResultMessage) connection.getResponse(statementTimeOut).getMessage();
				return resultMessage;
			}catch (UnpreparedException e) {
				logger.warn(e.getMessage());
				logger.warn("Trying to reprepare query...");
				reprepareQuery();
				if(--retries == 0)
					throw new CassandraException("Cannot prepare statement");
			}
		}
	}

	@Override
	public void addBatch() throws SQLException {
		QueryMessage qm = new QueryMessage();
		qm.setQueryId(preparedResultMessage.getPreparedStatementData().getId());
		qm.setValues(new TreeMap<Integer, ValueType>(values));
		qm.setMessageType(QueryMessageType.EXECUTE);
		batchQueries.add(qm);
	}

	@Override
	public void clearParameters() throws SQLException {
		values.clear();
	}

	@Override
	public boolean execute() throws SQLException {
		ResultMessage resultMessage = runExecute();
		if (resultMessage.getKind() == ResultKind.ROWS) {
			resultSet = new CassandraResultSet(this, resultMessage);
			return true;
		}
		return false;
	}

	@Override
	public ResultSet executeQuery() throws SQLException {
		// queryMessage.setQueryId(new byte[] {0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 0, 1, 0 ,0,
		// 1, 0});

		ResultMessage resultMessage = runExecute();
		resultSet = new CassandraResultSet(this, resultMessage);
		return resultSet;
	}

	@Override
	public int executeUpdate() throws SQLException {
		ResultMessage resultMessage = runExecute();
		if (resultMessage.getKind() == ResultKind.ROWS)
			throw new SQLException("executeUpdate() method cannot be used with SELECT statement");
		logger.warn(
				"executeUpdate() always return 0, because Cassandra doesnt return any information about insert/update");
		return 0;
	}

	@Override
	public ResultSetMetaData getMetaData() throws SQLException {
		return resultSet.getMetaData();
	}

	@Override
	public ParameterMetaData getParameterMetaData() throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void setArray(int arg0, Array arg1) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void setAsciiStream(int arg0, InputStream arg1) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void setAsciiStream(int arg0, InputStream arg1, int arg2) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void setAsciiStream(int arg0, InputStream arg1, long arg2) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void setBigDecimal(int arg0, BigDecimal arg1) throws SQLException {
		values.put(arg0, new ValueType(new CQLTypeMetadata(CQLType.DECIMAL), arg1));
	}

	@Override
	public void setBinaryStream(int arg0, InputStream arg1) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void setBinaryStream(int arg0, InputStream arg1, int arg2) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void setBinaryStream(int arg0, InputStream arg1, long arg2) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void setBlob(int arg0, Blob arg1) throws SQLException {
		values.put(arg0, new ValueType(new CQLTypeMetadata(CQLType.BLOB), arg1));

	}

	@Override
	public void setBlob(int arg0, InputStream arg1) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void setBlob(int arg0, InputStream arg1, long arg2) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void setBoolean(int arg0, boolean arg1) throws SQLException {
		values.put(arg0, new ValueType(new CQLTypeMetadata(CQLType.BOOLEAN), arg1));
	}

	@Override
	public void setByte(int arg0, byte arg1) throws SQLException {
		values.put(arg0, new ValueType(new CQLTypeMetadata(CQLType.TINYINT), arg1));

	}

	@Override
	public void setBytes(int arg0, byte[] arg1) throws SQLException {
		Blob b = new SerialBlob(arg1);
		values.put(arg0, new ValueType(new CQLTypeMetadata(CQLType.DECIMAL), arg1));
	}

	@Override
	public void setCharacterStream(int arg0, Reader arg1) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void setCharacterStream(int arg0, Reader arg1, int arg2) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void setCharacterStream(int arg0, Reader arg1, long arg2) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void setClob(int arg0, Clob arg1) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void setClob(int arg0, Reader arg1) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void setClob(int arg0, Reader arg1, long arg2) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void setDate(int arg0, Date arg1) {
		values.put(arg0, new ValueType(new CQLTypeMetadata(CQLType.DATE), arg1));
	}

	@Override
	public void setDate(int arg0, Date arg1, Calendar arg2) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void setDouble(int arg0, double arg1) throws SQLException {
		values.put(arg0, new ValueType(new CQLTypeMetadata(CQLType.DOUBLE), arg1));
	}

	@Override
	public void setFloat(int arg0, float arg1) throws SQLException {
		values.put(arg0, new ValueType(new CQLTypeMetadata(CQLType.DECIMAL), arg1));
	}

	@Override
	public void setInt(int arg0, int arg1) throws SQLException {
		values.put(arg0, new ValueType(new CQLTypeMetadata(CQLType.INT), arg1));
	}

	@Override
	public void setLong(int arg0, long arg1) throws SQLException {
		values.put(arg0, new ValueType(new CQLTypeMetadata(CQLType.BIGINT), arg1));
	}

	@Override
	public void setNCharacterStream(int arg0, Reader arg1) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void setNCharacterStream(int arg0, Reader arg1, long arg2) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void setNClob(int arg0, NClob arg1) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void setNClob(int arg0, Reader arg1) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void setNClob(int arg0, Reader arg1, long arg2) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void setNString(int arg0, String arg1) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void setNull(int arg0, int arg1) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void setNull(int arg0, int arg1, String arg2) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void setObject(int arg0, Object arg1) throws SQLException {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		ObjectOutputStream oos;
		try {
			oos = new ObjectOutputStream(baos);
			oos.writeObject(arg1);
			oos.flush();
			Blob b = new SerialBlob(baos.toByteArray());
			values.put(arg0, new ValueType(new CQLTypeMetadata(CQLType.DECIMAL), b));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	@Override
	public void setObject(int arg0, Object arg1, int arg2) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void setObject(int arg0, Object arg1, int arg2, int arg3) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void setRef(int arg0, Ref arg1) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void setRowId(int arg0, RowId arg1) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void setSQLXML(int arg0, SQLXML arg1) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void setShort(int arg0, short arg1) throws SQLException {
		values.put(arg0, new ValueType(new CQLTypeMetadata(CQLType.SMALLINT), arg1));
	}

	@Override
	public void setString(int arg0, String arg1) throws SQLException {
		values.put(arg0, new ValueType(new CQLTypeMetadata(CQLType.VARCHAR), arg1));
	}

	@Override
	public void setTime(int arg0, Time arg1) throws SQLException {
		values.put(arg0, new ValueType(new CQLTypeMetadata(CQLType.TIME), arg1));
	}

	@Override
	public void setTime(int arg0, Time arg1, Calendar arg2) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void setTimestamp(int arg0, Timestamp arg1) {
		values.put(arg0, new ValueType(new CQLTypeMetadata(CQLType.TIMESTAMP), arg1));
	}

	@Override
	public void setTimestamp(int arg0, Timestamp arg1, Calendar arg2) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void setURL(int arg0, URL arg1) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void setUnicodeStream(int arg0, InputStream arg1, int arg2) throws SQLException {
		// TODO Auto-generated method stub

	}

}
