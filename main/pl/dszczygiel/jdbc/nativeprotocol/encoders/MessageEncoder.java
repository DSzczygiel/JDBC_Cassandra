package pl.dszczygiel.jdbc.nativeprotocol.encoders;

import pl.dszczygiel.jdbc.nativeprotocol.encoders.framelayout.FrameLayoutEncoders;
import pl.dszczygiel.jdbc.nativeprotocol.frame.Message;

public abstract class MessageEncoder implements Encoder<Message>{
	FrameLayoutEncoders encoders = new FrameLayoutEncoders();
}
