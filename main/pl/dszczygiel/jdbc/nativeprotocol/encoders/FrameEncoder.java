package pl.dszczygiel.jdbc.nativeprotocol.encoders;

import java.io.ByteArrayOutputStream;

import pl.dszczygiel.jdbc.nativeprotocol.compression.Compression;
import pl.dszczygiel.jdbc.nativeprotocol.compression.NoCompression;
import pl.dszczygiel.jdbc.nativeprotocol.constants.OpCode;
import pl.dszczygiel.jdbc.nativeprotocol.encoders.framelayout.FrameLayoutEncoders;
import pl.dszczygiel.jdbc.nativeprotocol.frame.Frame;

public class FrameEncoder implements Encoder<Frame>{
	private Compression compression;
	public FrameEncoder(Compression compression) {
		this.compression = compression;
	}
	@Override
	public byte[] encode(Frame value) {
		Compression tmpComp = compression;
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		FrameLayoutEncoders encoders = new FrameLayoutEncoders();
		FrameHeaderEncoder headerEncoder = new FrameHeaderEncoder();
		MessageEncodersFactory encodersFactory = new MessageEncodersFactory();
		MessageEncoder messageEncoder = encodersFactory.getEncoder(value.getHeader().getOpCode());
		
		if(value.getHeader().getOpCode() == OpCode.STARTUP) {
			tmpComp = new NoCompression();
		}
		byte[] header = headerEncoder.encode(value.getHeader());
		byte[] message = messageEncoder.encode(value.getMessage());
		byte[] compressedMessage = tmpComp.compress(message);
		byte[] messageLen = encoders.encodeInt(compressedMessage.length);
		
		baos.write(header, 0, header.length);
		baos.write(messageLen, 0, messageLen.length);
		baos.write(compressedMessage, 0, compressedMessage.length);
		

	//	byte[] frameBytes = new byte[header.length + message.length];
		
		//System.arraycopy(header, 0, frameBytes, 0, header.length);
		//System.arraycopy(message, 0, frameBytes, header.length, message.length);
		
		return baos.toByteArray();
	}

}
