package pl.dszczygiel.jdbc.nativeprotocol.encoders.framelayout;

import java.io.ByteArrayOutputStream;
import java.util.Map;

import pl.dszczygiel.jdbc.driver.exceptions.CQLSerializerException;
import pl.dszczygiel.jdbc.nativeprotocol.constants.CQLType;
import pl.dszczygiel.jdbc.nativeprotocol.constants.Consistency;

public class FrameLayoutEncoders {

	public byte[] encodeInt(int intValue) {
		byte[] val = new byte[4];
		val[0] = (byte) (intValue >>> 24);
		val[1] = (byte) (intValue >>> 16);
		val[2] = (byte) (intValue >>> 8);
		val[3] = (byte) intValue;

		return val;
	}

	public byte[] encodeLong(long longValue) {
		byte[] val = new byte[8];
		val[0] = (byte) (longValue >>> 56);
		val[1] = (byte) (longValue >>> 48);
		val[2] = (byte) (longValue >>> 40);
		val[3] = (byte) (longValue >>> 32);
		val[4] = (byte) (longValue >>> 24);
		val[5] = (byte) (longValue >>> 16);
		val[6] = (byte) (longValue >>> 8);
		val[7] = (byte) longValue;

		return val;
	}

	public byte[] encodeLongString(String stringValue) {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();

		if (stringValue == null) {
			byte[] nullVal = encodeInt(-1);
			return nullVal;
		}

		int strLen = stringValue.length();
		byte[] strLenBytes = encodeInt(strLen);

		baos.write(strLenBytes, 0, strLenBytes.length);
		baos.write(stringValue.getBytes(), 0, strLen);

		return baos.toByteArray();
	}

	public byte[] encodeConsistency(Consistency consistency) {
		if (consistency == null)
			consistency = Consistency.ONE;

		byte[] consistencyBytes = new byte[2];
		consistencyBytes[0] = (byte) (consistency.ordinal() >> 8);
		consistencyBytes[1] = (byte) consistency.ordinal();

		return consistencyBytes;
	}

	public byte[] encodeShort(short shortValue) {
		byte[] shortBytes = new byte[2];
		shortBytes[0] = (byte) (shortValue >>> 8);
		shortBytes[1] = (byte) shortValue;

		return shortBytes;
	}

	public byte[] encodeString(String stringValue) {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		if (stringValue == null) {
			baos.write(0xFF);
			baos.write(0xFF);
		} else {
			short strLen = (short) stringValue.length();
			byte[] strLenBytes = encodeShort(strLen);
			baos.write(strLenBytes, 0, strLenBytes.length);
			baos.write(stringValue.getBytes(), 0, strLen);

		}
		return baos.toByteArray();
	}

	public byte[] encodeBytes(byte[] bytes) {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		if (bytes == null) {
			baos.write(0xFF);
			baos.write(0xFF);
			baos.write(0xFF);
			baos.write(0xFF);
			return baos.toByteArray();
		} else if (bytes.length == 0) {
			baos.write(0x00);
			baos.write(0x00);
			baos.write(0x00);
			baos.write(0x00);
			return baos.toByteArray();
		} else {
			byte[] intLen = encodeInt(bytes.length);
			baos.write(intLen, 0, intLen.length);
			for (byte b : bytes)
				baos.write(b);
			return baos.toByteArray();
		}
	}

	public byte[] encodeShortBytes(byte[] bytes) {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		if (bytes == null) {
			baos.write(0xFF);
			baos.write(0xFF);
			return baos.toByteArray();
		} else if (bytes.length == 0) {
			baos.write(0x00);
			baos.write(0x00);
			return baos.toByteArray();
		} else {
			byte[] shortLen = encodeShort((short) bytes.length);
			baos.write(shortLen, 0, shortLen.length);
			for (byte b : bytes)
				baos.write(b);
			return baos.toByteArray();
		}
	}

	public byte[] encodeStringMap(Map<String, String> stringMap) {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();

		if (stringMap == null) {
			baos.write(0xFF);
			baos.write(0xFF);
			return baos.toByteArray();
		}

		byte[] mapLen = encodeShort((short) stringMap.size());
		baos.write(mapLen, 0, mapLen.length);
		for (Map.Entry<String, String> entry : stringMap.entrySet()) {
			byte[] keyBytes = encodeString(entry.getKey());
			byte[] valueBytes = encodeString(entry.getValue());
			baos.write(keyBytes, 0, keyBytes.length);
			baos.write(valueBytes, 0, valueBytes.length);
		}

		return baos.toByteArray();
	}

	// TODO 'not set' value
	public byte[] encodeValue(ValueType value) {
		byte[] bytesValue = null;
		byte[] valueLen = null;

		if (value.getValue() == null) {
			valueLen = encodeInt(-1);
			return valueLen;
		}
		try {
			CQLSerializer serializer = new CQLSerializer();
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			bytesValue = serializer.serialize(value.getType(), value.getValue());
			valueLen = encodeInt(bytesValue.length);
			baos.write(valueLen, 0, valueLen.length);
			baos.write(bytesValue, 0, bytesValue.length);
			return baos.toByteArray();
		} catch (CQLSerializerException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return null;
	}
}
