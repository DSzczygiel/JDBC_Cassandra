package pl.dszczygiel.jdbc.nativeprotocol.decoders;

import java.nio.charset.StandardCharsets;

import pl.dszczygiel.jdbc.driver.exceptions.CassandraException;
import pl.dszczygiel.jdbc.driver.exceptions.ErrorCode;
import pl.dszczygiel.jdbc.driver.exceptions.InvalidQueryException;
import pl.dszczygiel.jdbc.driver.exceptions.SyntaxErrorException;
import pl.dszczygiel.jdbc.driver.exceptions.UnpreparedException;
import pl.dszczygiel.jdbc.nativeprotocol.frame.Message;

public class ErrorMessageDecoder extends MessageDecoder {
	private FrameLayoutDecoders decoders = new FrameLayoutDecoders();
	int currentPos = 0;

	public ErrorMessageDecoder(boolean readWarnings) {
		super(readWarnings);
	}

	@Override
	public Message decode(byte[] value) {
		int code = decoders.decodeInt(value, currentPos, 4);
		currentPos += 4;
		ErrorCode errorCode = ErrorCode.getByCode(code);
		int messageLen = decoders.decodeShort(value, currentPos);
		currentPos += 2;
		String message = decoders.decodeString(value, currentPos, messageLen, StandardCharsets.UTF_8);

		switch (errorCode) {
		case INVALID_QUERY:
			throw new InvalidQueryException(message);
		case SYNTAX_ERROR:
			throw new SyntaxErrorException(message);
		case UNPREPARED:
			throw new UnpreparedException(message);
		default:
			throw new CassandraException("Unknown or unimplemented error: " + message);
		}
	}
}
