package pl.dszczygiel.jdbc.nativeprotocol.frame;

import pl.dszczygiel.jdbc.nativeprotocol.constants.OpCode;

public class Header {
	public static final int HEADER_SIZE_BYTES =  9;
	private Integer protocolVersion;
	private Integer streamID;
	private HeaderFlags flags;
	private OpCode opCode;
	private int messageLength;
	
	public Header() {
		this.protocolVersion = 4;
		this.streamID = 0;
		this.flags = new HeaderFlags();
		this.opCode = OpCode.STARTUP;
		this.messageLength = 0;
	}
	
	
	public Integer getProtocolVersion() {
		return protocolVersion;
	}

	public void setProtocolVersion(Integer protocolVersion) {
		this.protocolVersion = protocolVersion;
	}

	public Integer getStreamID() {
		return streamID;
	}

	public void setStreamID(Integer streamID) {
		this.streamID = streamID;
	}

	public HeaderFlags getFlags() {
		return flags;
	}

	public void setFlags(HeaderFlags flags) {
		this.flags = flags;
	}

	public OpCode getOpCode() {
		return opCode;
	}

	public void setOpCode(OpCode opCode) {
		this.opCode = opCode;
	}

	public int getMessageLength() {
		return messageLength;
	}

	public void setMessageLength(int messageLength) {
		this.messageLength = messageLength;
	}
}
