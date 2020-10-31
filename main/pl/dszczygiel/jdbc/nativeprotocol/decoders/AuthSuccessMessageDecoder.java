package pl.dszczygiel.jdbc.nativeprotocol.decoders;

import pl.dszczygiel.jdbc.nativeprotocol.frame.Message;
import pl.dszczygiel.jdbc.nativeprotocol.message.responses.AuthSuccessMessage;

public class AuthSuccessMessageDecoder extends MessageDecoder{

	public AuthSuccessMessageDecoder(boolean readWarnings) {
		super(readWarnings);
	}

	@Override
	public Message decode(byte[] value) {
		AuthSuccessMessage asm = new AuthSuccessMessage();
		int bytesLen = decoders.decodeInt(value, 0, 4);
		byte[] messageBytes = decoders.decodeBytes(value, 4, bytesLen);
		asm.setRawAuthInfo(messageBytes);
		
		return asm;
	}

}
