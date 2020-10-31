package pl.dszczygiel.jdbc.driver;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.SocketException;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

import org.slf4j.LoggerFactory;

import pl.dszczygiel.jdbc.driver.exceptions.CQLException;
import pl.dszczygiel.jdbc.nativeprotocol.compression.Compression;
import pl.dszczygiel.jdbc.nativeprotocol.compression.LZ4Compression;
import pl.dszczygiel.jdbc.nativeprotocol.compression.NoCompression;
import pl.dszczygiel.jdbc.nativeprotocol.compression.SnappyCompression;
import pl.dszczygiel.jdbc.nativeprotocol.constants.CompressionAlgorithms;
import pl.dszczygiel.jdbc.nativeprotocol.constants.Consistency;
import pl.dszczygiel.jdbc.nativeprotocol.constants.EventType;
import pl.dszczygiel.jdbc.nativeprotocol.constants.OpCode;
import pl.dszczygiel.jdbc.nativeprotocol.decoders.FrameDecoder;
import pl.dszczygiel.jdbc.nativeprotocol.encoders.FrameEncoder;
import pl.dszczygiel.jdbc.nativeprotocol.frame.Frame;
import pl.dszczygiel.jdbc.nativeprotocol.frame.Header;
import pl.dszczygiel.jdbc.nativeprotocol.frame.Message;
import pl.dszczygiel.jdbc.nativeprotocol.message.responses.AuthChallengeMessage;
import pl.dszczygiel.jdbc.nativeprotocol.message.responses.AuthSuccessMessage;
import pl.dszczygiel.jdbc.nativeprotocol.message.responses.EventMessage;
import pl.dszczygiel.jdbc.nativeprotocol.message.responses.ResultMessage;
import pl.dszczygiel.jdbc.nativeprotocol.messages.requests.AuthResponseMessage;
import pl.dszczygiel.jdbc.nativeprotocol.messages.requests.BatchMessage;
import pl.dszczygiel.jdbc.nativeprotocol.messages.requests.QueryMessage;
import pl.dszczygiel.jdbc.nativeprotocol.messages.requests.RegisterMessage;
import pl.dszczygiel.jdbc.nativeprotocol.messages.requests.StartupMessage;
import pl.dszczygiel.jdbc.nativeprotocol.messages.requests.QueryMessage.QueryMessageType;
import pl.dszczygiel.jdbc.system.SystemPeers;
import pl.dszczygiel.jdbc.utils.Communication;
import pl.dszczygiel.jdbc.utils.URLParser;
import pl.dszczygiel.jdbc.utils.URLParser.ParsedURL;

public class CassandraConnection implements Connection {
	private static final org.slf4j.Logger logger = LoggerFactory.getLogger(CassandraConnection.class);

	private CassandraDatabaseMetaData databaseMetaData;
	private boolean isClosed = true;
	private int timeOutSeconds = 10;
	private Socket socket;
	private OutputStream os;
	private InputStream is;
	private Header header;
	private FrameEncoder frameEncoder;
	private FrameDecoder frameDecoder = new FrameDecoder(new NoCompression());
	private Properties info;
	public SchemaChangeListener schemaChangeListener; // TODO change to priv
	private TopologyChangeListener topologyChangeListener;
	private StatusChangeListener statusChangeListener;
	protected CassandraConnectionProperties connectionProperties;
	private Authenticator authenticator;
	private SystemPeers systemPeers;

	public CassandraConnection(String url, Properties info) throws UnknownHostException, IOException, SQLException {
		connectionProperties = new CassandraConnectionProperties();
		this.info = info;
		readProperties(url, info);
		init();
	}
	
	public CassandraConnection(String url, CassandraConnectionProperties properties) throws UnknownHostException, IOException, SQLException {
		connectionProperties = properties;
		readProperties(url);
		init();
	}

	
	private void init() throws IOException, SQLException {
		Compression comp;
		switch (connectionProperties.getCompression()) {
		case LZ4:
			comp = new LZ4Compression();
			break;
		case SNAPPY:
			comp = new SnappyCompression();
			break;
		default:
			comp = new NoCompression();
			break;
		}
		frameEncoder = new FrameEncoder(comp);
		header = new Header();

		socket = new Socket(connectionProperties.getHost(), connectionProperties.getPort());
		if (!socket.isConnected())
			throw new SocketException("Could not connect to Socket");
		isClosed = false;
		os = socket.getOutputStream();
		is = socket.getInputStream();
		// TODO reconnections
		authenticator = new TextPasswordAuthenticator(connectionProperties.getUsername(), connectionProperties.getPassword());

		initConnection();
		registerEvents();
		systemPeers = getPeersInfo();
		databaseMetaData = getDatabaseInfo();
		setKeyspace(connectionProperties.getKeyspace());
	}
	
	private void initConnection() {
		StartupMessage sm = new StartupMessage();
		sendMessage(sm, OpCode.STARTUP);
		Frame f = getResponse(timeOutSeconds);
	}

	private void readProperties(String url) throws SQLException {
		ParsedURL parsedUrl = null;
		try {
			parsedUrl = URLParser.parse(url);
		} catch (URISyntaxException e) {
			e.printStackTrace();
			throw new SQLException("Invalid connection string");
		}
		
		connectionProperties.setHost(parsedUrl.getHost());
		connectionProperties.setPort(parsedUrl.getPort());
		connectionProperties.setKeyspace(parsedUrl.getPath());
		if(connectionProperties.getKeyspace() == null || connectionProperties.getKeyspace().isEmpty()) {
			throw new SQLException("Specify keyspace in connection string");
		}

		String defaultConsistency = readProperty(parsedUrl, "defaultConsistency");
		if (defaultConsistency == null) {
			connectionProperties.setDefaultConsistency(Consistency.ONE);
			logger.warn("defaultConsistency parameter not set. Default setting is ONE.");
		} else {
			connectionProperties.setDefaultConsistency(Consistency.getByName(defaultConsistency));
		}

		String user = readProperty(parsedUrl, "username");
		connectionProperties.setUsername(user);
		String password = readProperty(parsedUrl, "password");
		connectionProperties.setPassword(password);
		String compression = readProperty(parsedUrl,  "compression");
		if (compression == null)
			connectionProperties.setCompression(CompressionAlgorithms.NO_COMPRESSION);
		else
			connectionProperties.setCompression(CompressionAlgorithms.getByName(compression));
		
	}
	
	private void readProperties(String url, Properties properties) throws SQLException {
		ParsedURL parsedUrl = null;
		try {
			parsedUrl = URLParser.parse(url);
		} catch (URISyntaxException e) {
			e.printStackTrace();
			throw new SQLException("Invalid connection string");
		}

		connectionProperties.setHost(parsedUrl.getHost());
		connectionProperties.setPort(parsedUrl.getPort());
		connectionProperties.setKeyspace(parsedUrl.getPath());
		if(connectionProperties.getKeyspace() == null || connectionProperties.getKeyspace().isEmpty()) {
			throw new SQLException("Specify keyspace in connection string");
		}

		String defaultConsistency = readProperty(parsedUrl, properties, "defaultConsistency");
		if (defaultConsistency == null) {
			connectionProperties.setDefaultConsistency(Consistency.ONE);
			logger.warn("defaultConsistency parameter not set. Default setting is ONE.");
		} else {
			connectionProperties.setDefaultConsistency(Consistency.getByName(defaultConsistency));
		}

		String user = readProperty(parsedUrl, properties, "username");
		connectionProperties.setUsername(user);
		String password = readProperty(parsedUrl, properties, "password");
		connectionProperties.setPassword(password);
		String compression = readProperty(parsedUrl, properties, "compression");
		if (compression == null)
			connectionProperties.setCompression(CompressionAlgorithms.NO_COMPRESSION);
		else
			connectionProperties.setCompression(CompressionAlgorithms.getByName(compression));
	}

	private String readProperty(ParsedURL url, Properties properties, String name) {
		String value = url.getParameterByName(name);
		if (value == null)
			value = properties.getProperty(name);

		return value;
	}
	
	private String readProperty(ParsedURL url, String name) {
		String value = url.getParameterByName(name);

		return value;
	}

	private void setKeyspace(String keyspace) {
		QueryMessage qm = new QueryMessage();
		qm.setConsistency(Consistency.ONE);
		qm.setQuery("USE " + keyspace);
		qm.setMessageType(QueryMessageType.QUERY);

		sendMessage(qm, OpCode.QUERY);
		getResponse(timeOutSeconds);
	}

	private void registerEvents() {
		RegisterMessage rm = new RegisterMessage();
		rm.addEvent(EventType.SCHEMA_CHANGE);
		rm.addEvent(EventType.STATUS_CHANGE);
		rm.addEvent(EventType.TOPOLOGY_CHANGE);

		sendMessage(rm, OpCode.REGISTER);
		getResponse(timeOutSeconds);
	}

	private CassandraDatabaseMetaData getDatabaseInfo() throws CQLException {
		QueryMessage qm = new QueryMessage();
		qm.setConsistency(Consistency.ONE);
		qm.setMessageType(QueryMessageType.QUERY);
		
		qm.setQuery("SELECT * FROM system_schema.tables");
		sendMessage(qm, OpCode.QUERY);
		Frame f = getResponse(timeOutSeconds);
		ResultMessage tablesRm = (ResultMessage) f.getMessage();
		
		qm.setQuery("SELECT * FROM system_schema.columns");
		sendMessage(qm, OpCode.QUERY);
		f = getResponse(timeOutSeconds);
		ResultMessage columnsRm = (ResultMessage) f.getMessage();
		
		qm.setQuery("SELECT * FROM system_schema.types");
		sendMessage(qm, OpCode.QUERY);
		f = getResponse(timeOutSeconds);
		ResultMessage typesRm = (ResultMessage) f.getMessage();
		
		return new CassandraDatabaseMetaData(columnsRm, tablesRm, typesRm);
	}

	private SystemPeers getPeersInfo() {
		QueryMessage qm = new QueryMessage();
		qm.setConsistency(Consistency.ONE);
		qm.setQuery("SELECT * FROM system.peers");
		qm.setMessageType(QueryMessageType.QUERY);
		sendMessage(qm, OpCode.QUERY);
		Frame f = getResponse(timeOutSeconds);

		ResultMessage rm = (ResultMessage) f.getMessage();
		return new SystemPeers(rm);
	}

	void sendMessage(Message message, OpCode opCode) {
		Frame frame = new Frame();
		header.setOpCode(opCode);
		frame.setHeader(header);
		frame.setMessage(message);
		try {
			Communication.sendData(this.os, frameEncoder.encode(frame));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	Frame getResponse(int timeOut) {
		timeOut *= 1000;
		byte[] responseBytes = null;
		try {
			this.socket.setSoTimeout(timeOut);
			responseBytes = Communication.getData(this.is);
			Frame f = frameDecoder.decode(responseBytes);
			OpCode opCode = f.getHeader().getOpCode();

			if (opCode == OpCode.EVENT) {
				handleEvent((EventMessage) f.getMessage());
				return getResponse(timeOut);
			}

			if (opCode == OpCode.AUTHENTICATE || opCode == OpCode.AUTH_CHALLENGE || opCode == OpCode.AUTH_SUCCESS) {
				handleAuthentication(f);
			}

			return f;
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}
	}

	private void handleEvent(EventMessage message) {
		if (message.getEventType() == EventType.SCHEMA_CHANGE) {
			if (schemaChangeListener != null) {
				schemaChangeListener.onSchemaChange(message.getSchemaChangeData());
			}
		} else if (message.getEventType() == EventType.TOPOLOGY_CHANGE) {
			if (topologyChangeListener != null) {
				topologyChangeListener.onTopologyChange(message.getTopologyChangeData());
			}
		} else if (message.getEventType() == EventType.STATUS_CHANGE) {
			if (statusChangeListener != null) {
				statusChangeListener.onStatusChange(message.getStatusChangeData());
			}
		}
	}

	private void handleAuthentication(Frame frame) {
		AuthResponseMessage arm = new AuthResponseMessage();

		switch (frame.getHeader().getOpCode()) {
		case AUTHENTICATE: {
			arm.setRawAuthResponse(authenticator.onAuthInit());
			sendMessage(arm, OpCode.AUTH_RESPONSE);
			getResponse(timeOutSeconds);
			break;
		}
		case AUTH_CHALLENGE: {
			AuthChallengeMessage acm = (AuthChallengeMessage) frame.getMessage();
			arm.setRawAuthResponse(authenticator.onAuthChallenge(acm.getRawChallengeBytes()));
			sendMessage(arm, OpCode.AUTH_RESPONSE);
			getResponse(timeOutSeconds);
			break;
		}
		case AUTH_SUCCESS: {
			AuthSuccessMessage asm = (AuthSuccessMessage) frame.getMessage();
			authenticator.onAuthSuccess(asm.getRawAuthInfo());
			break;
		}
		default:
			break;

		}
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
	public void abort(Executor arg0) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void clearWarnings() throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void close() throws SQLException {
		try {
			os.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	@Override
	public void commit() throws SQLException {
		throw new SQLException("Cannot do commit. Cassandra doesn't support transactions");
	}

	@Override
	public Array createArrayOf(String arg0, Object[] arg1) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Blob createBlob() throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Clob createClob() throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public NClob createNClob() throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public SQLXML createSQLXML() throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Statement createStatement() throws SQLException {
		return new CassandraStatement(this);
	}

	@Override
	public Statement createStatement(int arg0, int arg1) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Statement createStatement(int arg0, int arg1, int arg2) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Struct createStruct(String arg0, Object[] arg1) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean getAutoCommit() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public String getCatalog() throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Properties getClientInfo() throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getClientInfo(String arg0) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int getHoldability() throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public DatabaseMetaData getMetaData() throws SQLException {
		return databaseMetaData;
	}

	@Override
	public int getNetworkTimeout() throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public String getSchema() throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int getTransactionIsolation() throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public Map<String, Class<?>> getTypeMap() throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public SQLWarning getWarnings() throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean isClosed() throws SQLException {
		return isClosed;
	}

	@Override
	public boolean isReadOnly() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean isValid(int arg0) throws SQLException {
		return true;
	}

	@Override
	public String nativeSQL(String arg0) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public CallableStatement prepareCall(String arg0) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public CallableStatement prepareCall(String arg0, int arg1, int arg2) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public CallableStatement prepareCall(String arg0, int arg1, int arg2, int arg3) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public PreparedStatement prepareStatement(String arg0) throws SQLException {
		return new CassandraPreparedStatement(this, arg0);
	}

	@Override
	public PreparedStatement prepareStatement(String arg0, int arg1) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public PreparedStatement prepareStatement(String arg0, int[] arg1) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public PreparedStatement prepareStatement(String arg0, String[] arg1) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public PreparedStatement prepareStatement(String arg0, int arg1, int arg2) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public PreparedStatement prepareStatement(String arg0, int arg1, int arg2, int arg3) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void releaseSavepoint(Savepoint arg0) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void rollback() throws SQLException {
		throw new SQLException("Cannot do rollback. Cassandra doesn't support transactions.");

	}

	@Override
	public void rollback(Savepoint arg0) throws SQLException {
		throw new SQLException("Cannot do rollback. Cassandra doesn't support transactions.");
	}

	@Override
	public void setAutoCommit(boolean arg0) throws SQLException {
		throw new SQLException("Cannot set autocommit. Cassandra doesn't support transactions.");
	}

	@Override
	public void setCatalog(String arg0) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void setClientInfo(Properties arg0) throws SQLClientInfoException {
		// TODO Auto-generated method stub

	}

	@Override
	public void setClientInfo(String arg0, String arg1) throws SQLClientInfoException {
		// TODO Auto-generated method stub

	}

	@Override
	public void setHoldability(int arg0) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void setNetworkTimeout(Executor arg0, int arg1) throws SQLException {
		timeOutSeconds = arg1;
		try {
			socket.setSoTimeout(timeOutSeconds);
		} catch (SocketException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public void setReadOnly(boolean arg0) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public Savepoint setSavepoint() throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Savepoint setSavepoint(String arg0) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void setSchema(String arg0) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void setTransactionIsolation(int arg0) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void setTypeMap(Map<String, Class<?>> arg0) throws SQLException {
		// TODO Auto-generated method stub

	}
	
	public void setSchemaChangeListener(SchemaChangeListener listener) {
		this.schemaChangeListener = listener;
	}
	
	public int runBatch(BatchStatement batch) {
		BatchMessage message = new BatchMessage();
		message.setBatchType(batch.getBatchType());
		message.setConsistency(batch.getConsistency());
		message.setSerialConsistency(batch.getSerialConsistency());
		message.setQueries(batch.getStatements());
		message.setQueriesCount(batch.getStatements().size());
		message.setFlags(batch.getFlags());
		sendMessage(message, OpCode.BATCH);
			getResponse(timeOutSeconds);
		return 0;
	}

}
