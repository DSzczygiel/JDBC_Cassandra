package pl.dszczygiel.jdbc.nativeprotocol.messages.requests;

import java.util.HashMap;

import pl.dszczygiel.jdbc.nativeprotocol.constants.CompressionAlgorithms;
import pl.dszczygiel.jdbc.nativeprotocol.constants.CqlVersion;
import pl.dszczygiel.jdbc.nativeprotocol.frame.Message;

public class StartupMessage extends Message{
	private final String CQL_VERSION = "CQL_VERSION";
	private final String COMPRESSION = "COMPRESSION";
	private final String NO_COMPACT = "NO_COMPACT";
	private final String THROW_ON_OVERLOAD = "THROW_ON_OVERLOAD";
	
	private CqlVersion cqlVersion;
	private CompressionAlgorithms compressionAlgorithm;
	private boolean noCompatMode;
	private boolean throwOnOverload;
	HashMap<String, String> messages;	
	
	public StartupMessage() {
		 messages = new HashMap<String, String>();
		 messages.put(CQL_VERSION, "3.0.0");
	}
	
	
	public CqlVersion getCqlVersion() {
		return cqlVersion;
	}


	public void setCqlVersion(CqlVersion cqlVersion) {
		this.cqlVersion = cqlVersion;
		messages.replace(CQL_VERSION, cqlVersion.getVersion());
	}


	public CompressionAlgorithms getCompressionAlgorithm() {
		return compressionAlgorithm;
	}


	public void setCompressionAlgorithm(CompressionAlgorithms compressionAlgorithm) {
		this.compressionAlgorithm = compressionAlgorithm;
		messages.put(COMPRESSION, compressionAlgorithm.name());
	}


	public boolean isNoCompatMode() {
		return noCompatMode;
	}


	public void setNoCompatMode(boolean noCompatMode) {
		this.noCompatMode = noCompatMode;
		messages.put(NO_COMPACT, noCompatMode?"TRUE":"FALSE");
	}



	public boolean isThrowOnOverload() {
		return throwOnOverload;
	}



	public void setThrowOnOverload(boolean throwOnOverload) {
		this.throwOnOverload = throwOnOverload;
		messages.put(THROW_ON_OVERLOAD, throwOnOverload?"TRUE":"FALSE");
	}



	public HashMap<String, String> getMessages() {
		return messages;
	}

}
