package pl.dszczygiel.jdbc.nativeprotocol.decoders;

import java.nio.charset.StandardCharsets;

import pl.dszczygiel.jdbc.nativeprotocol.frame.Message;
import pl.dszczygiel.jdbc.nativeprotocol.message.responses.AuthenticateMessage;

public class AuthenticateMessageDecoder extends MessageDecoder {
	FrameLayoutDecoders decoders = new FrameLayoutDecoders();

	public AuthenticateMessageDecoder(boolean readWarnings) {
		super(readWarnings);
	}

	@Override
	public Message decode(byte[] value) {
		int currentPos = 0;
		AuthenticateMessage am = new AuthenticateMessage();
		int strLen = decoders.decodeShort(value, currentPos);
		currentPos += 2;
		String authClassStr = decoders.decodeString(value, currentPos, strLen, StandardCharsets.UTF_8);
		am.setAuthenticatorClass(authClassStr);
		return am;
	}

}
