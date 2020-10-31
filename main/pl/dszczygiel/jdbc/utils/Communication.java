package pl.dszczygiel.jdbc.utils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.sql.Connection;

import pl.dszczygiel.jdbc.nativeprotocol.frame.Frame;

public class Communication {
	
	public static void sendFrame(Frame frame, Connection connection) {
		
	}
	
	public static void sendData(OutputStream os, byte[] data) throws IOException {
		os.write(data);
	}
	
	public static byte[] getData(InputStream is) throws IOException {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		byte[] read = new byte[800000];//TODO change size
		int len;
		int dataRead = 0;
		int packetSize = 0;
		boolean first = true;
		
		while ((len = is.read(read)) != -1) {
			baos.write(read, 0, len);
			if(first) {
				first = false;
				packetSize = read[0] + read[1] + read[2]; //TODO read three bits
			}
			dataRead += len;
			if(dataRead >= packetSize)
				break;
		}
		byte[] bytes = baos.toByteArray();
		
		return bytes;
	}
}
