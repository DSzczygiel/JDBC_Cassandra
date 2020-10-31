package pl.dszczygiel.jdbc.nativeprotocol.encoders;

import pl.dszczygiel.jdbc.nativeprotocol.constants.OpCode;

public class MessageEncodersFactory{
	MessageEncoder getEncoder(OpCode opCode) {
		switch(opCode) {
		case STARTUP:
			return new StartupMessageEncoder();
		case QUERY:
			return new QueryMessageEncoder();
		case EXECUTE:
			return new QueryMessageEncoder();
		case PREPARE:
			return new PrepareMessageEncoder();
		case BATCH:
			return new BatchMessageEncoder();
		case REGISTER:
			return new RegisterMessageEncoder();
		case AUTH_RESPONSE:
			return new AuthResponseMessageEncoder();
		default:
			break;

		}
		throw new UnsupportedOperationException("No encoder for message type " + opCode.name());
	}
}
