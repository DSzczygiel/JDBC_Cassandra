package pl.dszczygiel.jdbc.nativeprotocol.frame;

public class Frame{
	private Header header;
	private Message message;
	
	public Header getHeader() {
		return header;
	}

	public void setHeader(Header header) {
		this.header = header;
	}

	public Message getMessage() {
		return message;
	}

	public void setMessage(Message message) {
		this.message = message;
	}
}
