package pl.dszczygiel.jdbc.nativeprotocol.message.responses;

import pl.dszczygiel.jdbc.nativeprotocol.frame.Message;

public class AuthChallengeMessage extends Message{
	private byte[] rawChallengeBytes;

	public byte[] getRawChallengeBytes() {
		return rawChallengeBytes;
	}

	public void setRawChallengeBytes(byte[] rawChallengeBytes) {
		this.rawChallengeBytes = rawChallengeBytes;
	}
	
	
}
