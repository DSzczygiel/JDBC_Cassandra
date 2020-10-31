package pl.dszczygiel.jdbc.nativeprotocol.decoders;

import java.util.Arrays;

import pl.dszczygiel.jdbc.nativeprotocol.compression.Compression;
import pl.dszczygiel.jdbc.nativeprotocol.compression.NoCompression;
import pl.dszczygiel.jdbc.nativeprotocol.frame.Frame;
import pl.dszczygiel.jdbc.nativeprotocol.frame.Header;
import pl.dszczygiel.jdbc.nativeprotocol.frame.Message;

public class FrameDecoder implements Decoder<Frame>{
	private Header header;
	private Message message;
	private FrameHeaderDecoder headerDecoder = new FrameHeaderDecoder();
	private MessageDecodersFactory decodersFactory = new MessageDecodersFactory();
	private MessageDecoder messageDecoder;
	private Compression compression;
	
	public FrameDecoder(Compression compression) {
		this.compression = compression;
	}
	
	@Override
	public Frame decode(byte[] responseBytes) {
		Frame frame = new Frame();
		header = headerDecoder.decode(responseBytes);
		frame.setHeader(header);
		Compression tmpComp = compression;
		if(!header.getFlags().getCompressionFlag()) {
			tmpComp = new NoCompression();
		}
		messageDecoder = decodersFactory.getDecoder(header.getOpCode(), header.getFlags().getWarningFlag());
		if(header.getMessageLength() == 0) {
			return frame;
		}
		byte[] messagePartBytes = Arrays.copyOfRange(responseBytes, Header.HEADER_SIZE_BYTES, responseBytes.length);
		byte[] decompressedMessageBytes = tmpComp.decompress(messagePartBytes);
		message = messageDecoder.decode(decompressedMessageBytes);
		frame.setMessage(message);
		
		return frame;
	}
	
	public Header getHeader() {
		return header;
	}
	public Message getMessage() {
		return message;
	}
	
	
}
