package pl.dszczygiel.jdbc.nativeprotocol.encoders;

import java.io.ByteArrayOutputStream;

import pl.dszczygiel.jdbc.nativeprotocol.constants.OpCode;
import pl.dszczygiel.jdbc.nativeprotocol.frame.Header;
import pl.dszczygiel.jdbc.nativeprotocol.frame.HeaderFlags;

public class FrameHeaderEncoder implements Encoder<Header>{

	@Override
	public byte[] encode(Header value) {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		HeaderFlags flags = value.getFlags();
		OpCode opCode = value.getOpCode();
		int streamId = value.getStreamID();
		
		byte flagsByte = 0;
		if(flags.getCompressionFlag())
			flagsByte |= 1;
		if(flags.getTracingFlag())
			flagsByte |= 2;
		if(flags.getCustomPayloadFlag())
			flagsByte |= 4;
		if(flags.getWarningFlag())
			flagsByte |= 8;
		
		baos.write(value.getProtocolVersion());	//protocolVersion
		baos.write(flagsByte);	//flags

		baos.write(streamId >>> 8);	//streamID
		baos.write(streamId);
		
		baos.write(opCode.value);	//opCode
		
		return baos.toByteArray();
	}

}
