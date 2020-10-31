package pl.dszczygiel.jdbc.nativeprotocol.message.responses;

import java.util.List;
import java.util.Map;

import pl.dszczygiel.jdbc.nativeprotocol.frame.Message;

public class SupportedMessage extends Message{
	private Map<String, List<String>> supportedOptions;

	public Map<String, List<String>> getSupportedOptions() {
		return supportedOptions;
	}

	public void setSupportedOptions(Map<String, List<String>> supportedOptions) {
		this.supportedOptions = supportedOptions;
	}	
}
