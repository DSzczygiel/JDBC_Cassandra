package pl.dszczygiel.jdbc.nativeprotocol.message.responses;

import pl.dszczygiel.jdbc.nativeprotocol.frame.Message;

public class AuthSuccessMessage extends Message{
	private byte[] rawAuthInfo;

	public byte[] getRawAuthInfo() {
		return rawAuthInfo;
	}

	public void setRawAuthInfo(byte[] rawAuthInfo) {
		this.rawAuthInfo = rawAuthInfo;
	}
}
