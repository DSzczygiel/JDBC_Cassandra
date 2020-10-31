package pl.dszczygiel.jdbc.nativeprotocol.frame;

import java.util.ArrayList;
import java.util.List;

import pl.dszczygiel.jdbc.nativeprotocol.constants.MessageType;

public abstract class Message {
	MessageType type;
	List<String> warnings = new ArrayList<String>();

	public MessageType getType() {
		return type;
	}

	public void setType(MessageType type) {
		this.type = type;
	}

	public List<String> getWarnings() {
		return warnings;
	}

	public void setWarnings(List<String> warnings) {
		this.warnings = warnings;
	}
	
}
