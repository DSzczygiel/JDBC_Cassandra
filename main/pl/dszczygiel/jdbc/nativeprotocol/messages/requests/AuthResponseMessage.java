package pl.dszczygiel.jdbc.nativeprotocol.messages.requests;

import pl.dszczygiel.jdbc.nativeprotocol.frame.Message;

public class AuthResponseMessage extends Message{
	private byte[] rawAuthResponse;

	public byte[] getRawAuthResponse() {
		return rawAuthResponse;
	}

	public void setRawAuthResponse(byte[] rawAuthResponse) {
		this.rawAuthResponse = rawAuthResponse;
	}
	
	
}
