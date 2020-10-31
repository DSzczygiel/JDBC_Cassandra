package pl.dszczygiel.jdbc.nativeprotocol.encoders;

import java.io.ByteArrayOutputStream;

import pl.dszczygiel.jdbc.nativeprotocol.encoders.framelayout.FrameStringMapEncoder;
import pl.dszczygiel.jdbc.nativeprotocol.frame.Message;
import pl.dszczygiel.jdbc.nativeprotocol.messages.requests.StartupMessage;

public class StartupMessageEncoder extends MessageEncoder{
	@Override
	public byte[] encode(Message message) {
		StartupMessage sm = (StartupMessage) message;
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		FrameStringMapEncoder mapEncoder = new FrameStringMapEncoder();//TODO change to layoutCodecs class
		
		byte[] map = mapEncoder.encode(sm.getMessages());
		Integer messageLength = map.length;
//
//		baos.write(messageLength >>> 8);	//message length
//		baos.write(messageLength >>> 16);
//		baos.write(messageLength >>> 24);
//		baos.write(messageLength);

		baos.write(map, 0, map.length);	//message

		
		return baos.toByteArray();
	}

}
