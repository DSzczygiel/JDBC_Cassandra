package pl.dszczygiel.jdbc.driver;

import java.net.InetAddress;

import pl.dszczygiel.jdbc.nativeprotocol.constants.CompressionAlgorithms;
import pl.dszczygiel.jdbc.nativeprotocol.constants.Consistency;

public class CassandraConnectionProperties {
	private InetAddress host;
	private int port;
	private CompressionAlgorithms compression;
	private String username;
	private String password;
	private Consistency defaultConsistency;
	private String keyspace;
	
	public InetAddress getHost() {
		return host;
	}
	public void setHost(InetAddress host) {
		this.host = host;
	}
	public int getPort() {
		return port;
	}
	public void setPort(int port) {
		this.port = port;
	}
	public CompressionAlgorithms getCompression() {
		return compression;
	}
	public void setCompression(CompressionAlgorithms compression) {
		this.compression = compression;
	}
	public String getUsername() {
		return username;
	}
	public void setUsername(String username) {
		this.username = username;
	}
	public String getPassword() {
		return password;
	}
	public void setPassword(String password) {
		this.password = password;
	}
	public Consistency getDefaultConsistency() {
		return defaultConsistency;
	}
	public void setDefaultConsistency(Consistency defaultConsistency) {
		this.defaultConsistency = defaultConsistency;
	}
	public String getKeyspace() {
		return keyspace;
	}
	public void setKeyspace(String keyspace) {
		this.keyspace = keyspace;
	}
	
	
}
