package pl.dszczygiel.jdbc.nativeprotocol.messages.requests;

import pl.dszczygiel.jdbc.nativeprotocol.frame.Message;

public class PrepareMessage extends Message{
	private String query;

	
	public PrepareMessage(String query) {
		super();
		this.query = query;
	}

	public String getQuery() {
		return query;
	}

	public void setQuery(String query) {
		this.query = query;
	}
}
