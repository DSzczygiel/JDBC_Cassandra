package pl.dszczygiel.jdbc.nativeprotocol.encoders;

import java.io.ByteArrayOutputStream;

import pl.dszczygiel.jdbc.nativeprotocol.constants.EventType;
import pl.dszczygiel.jdbc.nativeprotocol.encoders.framelayout.FrameLayoutEncoders;
import pl.dszczygiel.jdbc.nativeprotocol.frame.Message;
import pl.dszczygiel.jdbc.nativeprotocol.messages.requests.RegisterMessage;

public class RegisterMessageEncoder extends MessageEncoder{

	@Override
	public byte[] encode(Message value) {
		FrameLayoutEncoders encoders = new FrameLayoutEncoders();
		RegisterMessage rm = (RegisterMessage) value;
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		int eventsCount = rm.eventsCount();
		byte[] eventCountBytes = encoders.encodeShort((short)eventsCount);
		baos.write(eventCountBytes, 0, eventCountBytes.length);
		for(EventType event : rm.getEvents()) {
			byte[] eventName = encoders.encodeString(event.name());
			baos.write(eventName, 0, eventName.length);
		}
		return baos.toByteArray();
	}

}
