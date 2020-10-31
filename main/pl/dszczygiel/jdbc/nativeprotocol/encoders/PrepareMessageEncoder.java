package pl.dszczygiel.jdbc.nativeprotocol.encoders;

import pl.dszczygiel.jdbc.nativeprotocol.encoders.framelayout.FrameLayoutEncoders;
import pl.dszczygiel.jdbc.nativeprotocol.frame.Message;
import pl.dszczygiel.jdbc.nativeprotocol.messages.requests.PrepareMessage;

public class PrepareMessageEncoder extends MessageEncoder{

	@Override
	public byte[] encode(Message value) {
		FrameLayoutEncoders encoders = new FrameLayoutEncoders();
		PrepareMessage prepareMessage = (PrepareMessage) value;
		
		byte[] message = encoders.encodeLongString(prepareMessage.getQuery());
		return message;
	}

}
