package pl.dszczygiel.jdbc.nativeprotocol.decoders;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import pl.dszczygiel.jdbc.nativeprotocol.constants.CQLType;
import pl.dszczygiel.jdbc.nativeprotocol.types.CQLListSetTypeMetadata;
import pl.dszczygiel.jdbc.nativeprotocol.types.CQLTypeMetadata;
import pl.dszczygiel.jdbc.nativeprotocol.types.CQLUdtMetadata;
import pl.dszczygiel.jdbc.nativeprotocol.types.UserDefinedType;
import pl.dszczygiel.jdbc.utils.ArrayUtils;

public class CQLDeserializer {
	private final static HashMap<CQLType, Class> compatTypes = new HashMap<CQLType, Class>();
	static {
		compatTypes.put(CQLType.VARCHAR, String.class);
		compatTypes.put(CQLType.INT, Integer.class);
		compatTypes.put(CQLType.BIGINT, Long.class);
		compatTypes.put(CQLType.DOUBLE, Double.class);
		compatTypes.put(CQLType.FLOAT, Float.class);
	}
	
	private FrameLayoutDecoders decoders = new FrameLayoutDecoders();
	
	public Object deserialize(CQLTypeMetadata type, byte[] value) {
		switch(type.getType()) {
		case VARCHAR:
			return deserializeString(value);
		case ASCII:
			return deserializeString(value);
		case BIGINT:
			break;
		case BLOB:
			break;
		case BOOLEAN:
			break;
		case COUNTER:
			break;
		case CUSTOM:
			break;
		case DATE:
			break;
		case DECIMAL:
			return deserializeDecimal(value);
		case DOUBLE:
			return deserializeDouble(value);
		case FLOAT:
			break;
		case INT:
			return deserializeInt(value);
		case LIST:
			CQLType elementType =(((CQLListSetTypeMetadata) type).getElementsType());
			return deserializeList(value, elementType, compatTypes.get(elementType));
		case MAP:
			break;
		case SET:
			break;
		case SMALLINT:
			break;
		case TIME:
			break;
		case TIMESTAMP:
			break;
		case TIMEUUID:
			break;
		case TINYINT:
			break;
		case TUPLE:
			break;
		case UDT:
			return deserializeUDT(value, (CQLUdtMetadata) type);
		case UUID:
			break;
		case VARINT:
			return deserializeVarint(value);
		case INET:
			
		default:
			break;
		}
		return value;
	}
	
	private Object deserializeInet(byte[] value) throws UnknownHostException {
		return decoders.decodeInet(value, 0);
	}
	
	private Object deserializeString(byte[] value) {		
		String string = new String(value);
		return string;
	}
	
	private Object deserializeInt(byte[] value) {
		int i = decoders.decodeInt(value, 0, 4);
		return i;
	}
	
	private Object deserializeVarint(byte[] value) {
		BigInteger bigInteger = new BigInteger(value);
		return bigInteger;
	}
	
	private Object deserializeDecimal(byte[] value) {
		int scale = decoders.decodeInt(value, 0, 4);
		BigInteger bigInteger = new BigInteger(Arrays.copyOfRange(value, 4, value.length));
		BigDecimal bigDecimal = new BigDecimal(bigInteger, scale);
		
		return bigDecimal;
	}
	
	private Object deserializeDouble(byte[] value) {
	    return ByteBuffer.wrap(value).getDouble();
	}
	
	private <T> Object deserializeList(byte value[], CQLType elementsType, Class<T> c) {
		int currentPost = 0;
		int listSize = decoders.decodeInt(value, 0, 4);
		currentPost += 4;
		List<T> list = new ArrayList<>();
		
		for(int i=0; i<listSize; i++) {
			int len = decoders.decodeInt(value, currentPost, 4);
			currentPost+=4;
			byte[] val = Arrays.copyOfRange(value, currentPost, currentPost+len);
			T o = (T) deserialize(new CQLTypeMetadata(elementsType), val);
			currentPost += len;
			list.add(o);
		}
		
		return list;
	}
	
	private Object deserializeUDT(byte[] value, CQLUdtMetadata meta) {
		int currentPos = 0;
		UserDefinedType type = new UserDefinedType(meta);
		for(int i=0; i<meta.getFieldNames().size(); i++) {
			int len = decoders.decodeInt(value, currentPos, 4);
			currentPos+=4;
			byte[] valueBytes = decoders.decodeBytes(value, currentPos, len);
			Object o = deserialize(meta.getFieldTypes().get(i), valueBytes);
			currentPos+=valueBytes.length;
			type.getFields().put(meta.getFieldNames().get(i), o);
		}
		
		return type;
	}
}
