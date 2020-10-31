package pl.dszczygiel.jdbc.nativeprotocol.message.responses;

import pl.dszczygiel.jdbc.nativeprotocol.frame.Message;

public class AuthenticateMessage extends Message{
	private String authenticatorClass;

	public String getAuthenticatorClass() {
		return authenticatorClass;
	}

	public void setAuthenticatorClass(String authenticatorClass) {
		this.authenticatorClass = authenticatorClass;
	}
	
	

}
