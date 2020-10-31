package pl.dszczygiel.jdbc.nativeprotocol.decoders;

import pl.dszczygiel.jdbc.nativeprotocol.compression.Compression;
import pl.dszczygiel.jdbc.nativeprotocol.frame.Message;

public abstract class MessageDecoder implements Decoder<Message> {
	FrameLayoutDecoders decoders = new FrameLayoutDecoders();
	private Compression compressionAlgorithm;
	private boolean readWarnings;
	
	public MessageDecoder(boolean readWarnings) {
		this.readWarnings = readWarnings;
	}

	protected boolean isReadWarnings() {
		return readWarnings;
	}

	protected void setReadWarnings(boolean readWarnings) {
		this.readWarnings = readWarnings;
	}
	
	
}
