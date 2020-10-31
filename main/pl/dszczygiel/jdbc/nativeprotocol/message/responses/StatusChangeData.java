package pl.dszczygiel.jdbc.nativeprotocol.message.responses;

import java.net.InetAddress;

public class StatusChangeData {
	public static final String CHANGE_UP = "UP";
	public static final String CHANGE_DOWN = "DOWN";

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
