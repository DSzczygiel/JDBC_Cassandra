package pl.dszczygiel.jdbc.nativeprotocol.encoders;

import pl.dszczygiel.jdbc.nativeprotocol.frame.Message;
import pl.dszczygiel.jdbc.nativeprotocol.messages.requests.AuthResponseMessage;

public class AuthResponseMessageEncoder extends MessageEncoder{
	@Override
	public byte[] encode(Message value) {
		AuthResponseMessage arm = (AuthResponseMessage) value;
		byte[] messageBytes = encoders.encodeBytes(arm.getRawAuthResponse());
		return messageBytes;
	}

}
