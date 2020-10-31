package pl.dszczygiel.jdbc.nativeprotocol.message.responses;

import java.net.InetAddress;

public class TopologyChangeData {
	public static final String CHANGE_NEW_NODE = "NEW NODE";
	public static final String CHANGE_REMOVED_NODE = "REMOVED NODE";
	
	private String changeType;
	private InetAddress address;
	public String getChangeType() {
		return changeType;
	}
	public void setChangeType(String changeType) {
		this.changeType = changeType;
	}
	public InetAddress getAddress() {
		return address;
	}
	public void setAddress(InetAddress address) {
		this.address = address;
	}
	
	
}
