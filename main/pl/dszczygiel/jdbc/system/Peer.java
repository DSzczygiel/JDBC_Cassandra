package pl.dszczygiel.jdbc.system;

import java.net.InetAddress;
import java.util.Set;

public class Peer {
	private InetAddress peerAddress;
	private String rack;
	private String datacenter;
	private InetAddress preferedAddress;
	private Set<String> tokens;
	
	public InetAddress getPeerAddress() {
		return peerAddress;
	}
	public void setPeerAddress(InetAddress peerAddress) {
		this.peerAddress = peerAddress;
	}
	public String getRack() {
		return rack;
	}
	public void setRack(String rack) {
		this.rack = rack;
	}
	public String getDatacenter() {
		return datacenter;
	}
	public void setDatacenter(String datacenter) {
		this.datacenter = datacenter;
	}
	public InetAddress getPreferedAddress() {
		return preferedAddress;
	}
	public void setPreferedAddress(InetAddress preferedAddress) {
		this.preferedAddress = preferedAddress;
	}
	public Set<String> getTokens() {
		return tokens;
	}
	public void setTokens(Set<String> tokens) {
		this.tokens = tokens;
	}
	
	
}
