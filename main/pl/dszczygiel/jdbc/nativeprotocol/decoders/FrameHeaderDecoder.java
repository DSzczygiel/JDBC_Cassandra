package pl.dszczygiel.jdbc.nativeprotocol.decoders;

import pl.dszczygiel.jdbc.nativeprotocol.constants.OpCode;
import pl.dszczygiel.jdbc.nativeprotocol.frame.Header;
import pl.dszczygiel.jdbc.nativeprotocol.frame.HeaderFlags;

public class FrameHeaderDecoder implements Decoder<Header>{
	final int PROTOCOL_VERSION_SIZE = 1;
	final int FLAGS_SIZE = 1;
	final int STREAM_ID_SIZE = 2;
	final int OPCODE_SIZE = 1;
	final int MESSAGE_LENGTH_SIZE = 4;
	
	private FrameLayoutDecoders decoders = new FrameLayoutDecoders();
	@Override
	public Header decode(byte[] val) {
		int currentPos = 0;
		
		int protocolVersion = decoders.decodeByte(val, currentPos) & 0x0F;//Converter.byteArrayToInt(val, currentPos, currentPos + PROTOCOL_VERSION_SIZE - 1);
		currentPos += PROTOCOL_VERSION_SIZE;	//1
		
		byte flags = decoders.decodeByte(val, currentPos);//Converter.byteArrayToInt(val, currentPos, currentPos + FLAGS_SIZE - 1);
		currentPos += FLAGS_SIZE;	//2
		
		int streamId = decoders.decodeShort(val, currentPos);//Converter.byteArrayToInt(val, currentPos, currentPos + STREAM_ID_SIZE - 1);
		currentPos += STREAM_ID_SIZE;	//4
		
		int opCode = decoders.decodeByte(val, currentPos);//Converter.byteArrayToInt(val, currentPos, currentPos + OPCODE_SIZE - 1);
		currentPos += OPCODE_SIZE;
		
		int messageLength = decoders.decodeInt(val, currentPos, 4);//Converter.byteArrayToInt(val, currentPos, currentPos + MESSAGE_LENGTH_SIZE - 1);
		
		Header header = new Header();
		header.setProtocolVersion(protocolVersion);
		header.setStreamID(streamId);
		
		HeaderFlags f = new HeaderFlags();
		if((flags & 1) == 1) {
			f.setCompressionFlag(true);
		}
		if((flags & 2) == 2) {
			f.setTracingFlag(true);
		}
		if((flags & 4) == 4) {
			f.setCustomPayloadFlag(true);
		}
		if((flags & 8) == 8) {
			f.setWarningFlag(true);
		}
		header.setFlags(f);
		
		OpCode opCodeEnum = OpCode.getOpCode(opCode); //TODO catch null
		header.setOpCode(opCodeEnum);
		
		header.setMessageLength(messageLength);
		
		return header;
	}
	
}
