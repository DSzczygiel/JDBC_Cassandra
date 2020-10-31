package pl.dszczygiel.jdbc.nativeprotocol.decoders;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import pl.dszczygiel.jdbc.nativeprotocol.constants.CQLType;
import pl.dszczygiel.jdbc.nativeprotocol.types.CQLListSetTypeMetadata;
import pl.dszczygiel.jdbc.nativeprotocol.types.CQLMapTypeMetadata;
import pl.dszczygiel.jdbc.nativeprotocol.types.CQLTypeMetadata;
import pl.dszczygiel.jdbc.nativeprotocol.types.CQLUdtMetadata;

public class FrameLayoutDecoders {
	public int decodeInt(byte[] value) {
		return ByteBuffer.wrap(value).getInt();
	}

	public int decodeInt(byte[] value, int ofs, int len) {
		return ByteBuffer.wrap(value, ofs, len).getInt();
	}

	public long decodeLong(byte[] value) {
		return ByteBuffer.wrap(value).getLong();
	}

	public short decodeShort(byte[] value) {
		return ByteBuffer.wrap(value).getShort();
	}

	public short decodeShort(byte[] value, int ofs) {
		return ByteBuffer.wrap(value, ofs, 2).getShort();
	}

	public byte decodeByte(byte[] value, int ofs) {
		return ByteBuffer.wrap(value, ofs, 1).get();
	}

	public byte[] decodeBytes(byte[] value, int ofs, int len) {
		if (len < 0)
			return null;
		return Arrays.copyOfRange(value, ofs, ofs + len);
	}

	public String decodeString(byte[] value, int ofs, int len, Charset charset) {
		if (len == 0) {
			return "";
		} else if (len == -1) {
			return null;
		} else {
			byte[] stringBytes = Arrays.copyOfRange(value, ofs, ofs + len);
			return new String(stringBytes, charset);
		}
	}

	public InetAddress decodeInet(byte[] value, int ofs) throws UnknownHostException {
		StringBuilder sb = new StringBuilder();
		int currentPos = ofs;
		byte addrSize = decodeByte(value, currentPos);
		currentPos += 1;
		if (addrSize == 4) {
			for (int i = 0; i < 3; i++) {
				int addrByte = (decodeByte(value, currentPos) & 0xFF);
				currentPos++;
				sb.append(addrByte).append(".");
			}
			int addrByte = (decodeByte(value, currentPos) & 0xFF);
			currentPos++;
			sb.append(addrByte);
		} else if (addrSize == 16) {
			for (int i = 0; i < 15; i++) {
				int addrByte = (decodeByte(value, currentPos) & 0xFF);
				currentPos++;
				sb.append(String.format("%x", addrByte)).append(":");
			}
			int addrByte = (decodeByte(value, currentPos) & 0xFF);
			currentPos++;
			sb.append(String.format("%x", addrByte));
		}
		return InetAddress.getByName(sb.toString());
	}

	public int decodeStringList(byte[] value, int ofs, List<String> targetList, Charset charset) {
		int currentPos = ofs;
		int nrOfElements = decodeShort(value, currentPos);
		currentPos += 2;

		for (int i = 0; i < nrOfElements; i++) {
			int strLen = decodeShort(value, currentPos);
			currentPos += 2;
			String s = decodeString(value, currentPos, strLen, charset);
			if (strLen > 0)
				currentPos += strLen;
			targetList.add(s);
		}

		return currentPos - ofs;
	}

	public CQLTypeMetadata decodeCqlType(byte[] value, int ofs) {
		int currentPos = ofs;
		int typeId = decodeShort(value, currentPos);
		currentPos += 2;

		CQLType type = CQLType.getType(typeId);

		if (type == CQLType.LIST) {
			typeId = decodeShort(value, currentPos);
			currentPos += 2;
			CQLType elementsType = CQLType.getType(typeId);
			return new CQLListSetTypeMetadata(type, elementsType);
		} else if (type == CQLType.SET) {
			typeId = decodeShort(value, currentPos);
			currentPos += 2;
			CQLType elementsType = CQLType.getType(typeId);
			return new CQLListSetTypeMetadata(type, elementsType);
		} else if (type == CQLType.MAP) {
			typeId = decodeShort(value, currentPos);
			currentPos += 2;
			CQLType keyType = CQLType.getType(typeId);
			typeId = decodeShort(value, currentPos);
			currentPos += 2;
			CQLType valType = CQLType.getType(typeId);
			return new CQLMapTypeMetadata(type, keyType, valType);
		} else if (type == CQLType.TUPLE) {

		} else if (type == CQLType.UDT) {
			List<String> fieldNames = new ArrayList<String>();
			List<CQLTypeMetadata> metas = new ArrayList<CQLTypeMetadata>();
			int strLen = decodeShort(value, currentPos);
			currentPos+=2;
			String keyspace = decodeString(value, currentPos, strLen, StandardCharsets.UTF_8);
			currentPos+=keyspace.length();
			strLen = decodeShort(value, currentPos);
			currentPos+=2;
			String name = decodeString(value, currentPos, strLen, StandardCharsets.UTF_8);
			currentPos += name.length();
			int fieldsNr = decodeShort(value, currentPos);
			currentPos+=2;
			
			for(int i=0; i<fieldsNr; i++) {
				strLen = decodeShort(value, currentPos);
				currentPos+=2;
				String fieldName = decodeString(value, currentPos, strLen, StandardCharsets.UTF_8);
				currentPos+=fieldName.length();
				typeId = decodeShort(value, currentPos);
				currentPos+=2;
				fieldNames.add(fieldName);
				metas.add(new CQLTypeMetadata(CQLType.getType(typeId))); //TODO recursive call for collections
			}
			return new CQLUdtMetadata(type, keyspace, name, fieldNames, metas);

		} else {
			return new CQLTypeMetadata(type);
		}
		return null;
	}
}
