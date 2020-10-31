package pl.dszczygiel.jdbc.nativeprotocol.decoders;

import pl.dszczygiel.jdbc.nativeprotocol.compression.Compression;
import pl.dszczygiel.jdbc.nativeprotocol.constants.OpCode;

public class MessageDecodersFactory {
	public MessageDecoder getDecoder(OpCode opCode, boolean containsWarning) {
		switch (opCode) {
		case RESULT:
			return new ResultMessageDecoder(containsWarning);
		case EVENT:
			return new EventMessageDecoder(containsWarning);
		case ERROR:
			return new ErrorMessageDecoder(containsWarning);
		case AUTHENTICATE:
			return new AuthenticateMessageDecoder(containsWarning);
		case AUTH_SUCCESS:
			return new AuthSuccessMessageDecoder(containsWarning);
		case READY:
			return null;
		default:
			throw new UnsupportedOperationException("No decoder for message type " + opCode.name());
		}
	}
}
