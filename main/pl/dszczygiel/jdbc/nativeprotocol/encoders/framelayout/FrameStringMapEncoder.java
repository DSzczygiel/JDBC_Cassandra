package pl.dszczygiel.jdbc.nativeprotocol.encoders.framelayout;

import java.io.ByteArrayOutputStream;
import java.util.HashMap;
import java.util.Map;

import pl.dszczygiel.jdbc.nativeprotocol.encoders.Encoder;

public class FrameStringMapEncoder implements Encoder<HashMap<String, String>> {
	@Override
	public byte[] encode(HashMap<String, String> map) {
		Integer stringMapSize = map.size();
		ByteArrayOutputStream baos = new ByteArrayOutputStream();

		baos.write(stringMapSize >>> 8);
		baos.write(stringMapSize);

		for (Map.Entry<String, String> entry : map.entrySet()) {
			Integer keyLength = entry.getKey().length();
			Integer ValueLength = entry.getValue().length();
			byte[] keyBytes = entry.getKey().getBytes();
			byte[] valueBytes = entry.getValue().getBytes();

			baos.write(keyLength >>> 8);	//keyLength
			baos.write(keyLength);

			baos.write(keyBytes, 0, keyBytes.length);	//key

			baos.write(ValueLength >>> 8);	//valueLength
			baos.write(ValueLength);

			baos.write(valueBytes, 0, valueBytes.length);	//value
		}

		return baos.toByteArray();
	}
}
