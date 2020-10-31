package pl.dszczygiel.jdbc.nativeprotocol.encoders.framelayout;

import java.io.ByteArrayOutputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;

import pl.dszczygiel.jdbc.driver.exceptions.CQLSerializerException;
import pl.dszczygiel.jdbc.nativeprotocol.constants.CQLType;
import pl.dszczygiel.jdbc.nativeprotocol.types.CQLListSetTypeMetadata;
import pl.dszczygiel.jdbc.nativeprotocol.types.CQLTypeMetadata;
import pl.dszczygiel.jdbc.nativeprotocol.types.UserDefinedType;

public class CQLSerializer {
	private final static HashMap<Class, CQLType> compatTypes = new HashMap<Class, CQLType>();
	static {
		compatTypes.put(String.class, CQLType.VARCHAR);
		compatTypes.put(Integer.class, CQLType.INT);
	}
	
	private FrameLayoutEncoders encoders = new FrameLayoutEncoders();
	
	public byte[] serialize(CQLTypeMetadata type, Object value) throws CQLSerializerException {
		switch (type.getType()) {
		case INT:
			return serializeInteger((Integer) value);
		case SMALLINT:
			return serializeShort((Short) value);
		case TINYINT:
			return serializeByte((Byte) value);
		case BOOLEAN:
			return serializeBoolean((Boolean) value);
		case VARCHAR:
			return serializeString((String) value);
		case VARINT:
			return serializeBigInteger((BigInteger) value);
		case DATE:
			return serializeDate((Date) value);
		case BIGINT:
			return serializeLong((Long) value);
		case TIME:
			return serializeLong((Long) value);
		case ASCII:
			return serializeAsciiString((String) value);
		case BLOB:
			return serializeBytes((byte[]) value);
		case CUSTOM:
			return serializeString((String) value);
		case DOUBLE:
			return serializeDouble((Double) value);
		case FLOAT:
			return serializeFloat((Float) value);
		case DECIMAL:
			return serializeDecimal((BigDecimal) value);
		case TIMESTAMP:
			return serializeLong((Long) value);
		case UUID:
			return serializeUUID((UUID) value);
		case TIMEUUID:
			return serializeUUID((UUID) value);
		case LIST:
			return serializeList((List<?>) value, ((CQLListSetTypeMetadata) type).getElementsType());
		case UDT:
			return serializeUdt((UserDefinedType) value);
		default:
			throw new CQLSerializerException("Cannot serialize given type");
		}
	}

	private byte[] serializeString(String value) {
		byte[] serialized = value.getBytes(StandardCharsets.UTF_8);
		return serialized;
	}
	
	private byte[] serializeBytes(byte[] value) {
		return value;
	}
	
	private byte[] serializeAsciiString(String value) {
		byte[] serialized = value.getBytes(StandardCharsets.US_ASCII);
		return serialized;
	}
	
	private byte[] serializeInteger(Integer value) {
		byte[] serialized = new byte[4];
		serialized[0] = (byte) (value >> 24);
		serialized[1] = (byte) (value >> 16);
		serialized[2] = (byte) (value >> 8);
		serialized[3] = value.byteValue();

		return serialized;
	}

	private byte[] serializeLong(Long value) {
		byte[] serialized = new byte[8];
		serialized[0] = (byte) (value >> 56);
		serialized[1] = (byte) (value >> 48);
		serialized[2] = (byte) (value >> 40);
		serialized[3] = (byte) (value >> 32);
		serialized[4] = (byte) (value >> 24);
		serialized[5] = (byte) (value >> 16);
		serialized[6] = (byte) (value >> 8);
		serialized[7] = value.byteValue();

		return serialized;
	}
	
	private byte[] serializeShort(Short value) {
		byte[] serialized = new byte[2];
		serialized[0] = (byte) (value >> 8);
		serialized[1] = value.byteValue();

		return serialized;
	}

	private byte[] serializeByte(Byte value) {
		byte[] serialized = new byte[1];
		serialized[0] = value.byteValue();

		return serialized;
	}
	
	private byte[] serializeBoolean(Boolean value) {
		byte[] serialized = new byte[1];
		serialized[0] = (byte) (value?1:0);

		return serialized;
	}
	
	private byte[] serializeBigInteger(BigInteger value) {
		byte[] serialized = value.toByteArray();
		return serialized;
	}
	
	private byte[] serializeDate(Date date) {
		int dateInt = (int) date.getTime();
		byte[] serialized = serializeInteger(dateInt);
		return serialized;
	}
	
	private byte[] serializeFloat(Float value) {
		int intBits = Float.floatToIntBits(value);
		byte[] serialized = serializeInteger(intBits);

		return serialized;
	}
	
	private byte[] serializeDouble(Double value) {
		long longBits = Double.doubleToLongBits(value);
		byte[] serialized = serializeLong(longBits);
		
		return serialized;
	}
	
	private byte[] serializeDecimal(BigDecimal value) {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		int scale = value.scale();
		byte[] scaleBytes = serializeInteger(scale);
		byte[] unscaledBytes = value.unscaledValue().toByteArray();
		baos.write(scaleBytes, 0, scaleBytes.length);
		baos.write(unscaledBytes, 0, unscaledBytes.length);
		
		return baos.toByteArray();
	}
	
	private byte[] serializeUUID(UUID value) {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		byte[] msBytes = serializeLong(value.getMostSignificantBits());
		byte[] lsBytes = serializeLong(value.getLeastSignificantBits());
		
		baos.write(msBytes, 0, msBytes.length);
		baos.write(lsBytes, 0, lsBytes.length);

		return baos.toByteArray();
	}
	
	private byte[] serializeList(List<?> list, CQLType valuesType) throws CQLSerializerException {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		byte[] nrOfValuesBytes = serializeInteger(list.size());
		baos.write(nrOfValuesBytes, 0, nrOfValuesBytes.length);
		
		
		
		for(int i=0; i<list.size(); i++) {
			byte[] valueBytes = serialize(new CQLTypeMetadata(valuesType), list.get(i));
			byte[] valueLen = serializeInteger(valueBytes.length);
			baos.write(valueLen, 0, valueLen.length);
			baos.write(valueBytes, 0, valueBytes.length);

		}
		return baos.toByteArray();
	}
	
	private byte[] serializeUdt(UserDefinedType type) throws CQLSerializerException {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		for(int i=0; i<type.getFields().size(); i++) {
			String fieldName = type.getMeta().getFieldNames().get(i);
			CQLTypeMetadata fieldType = type.getMeta().getFieldTypes().get(i);
			
			byte[] valueBytes = serialize(fieldType, type.getFields().get(fieldName));
			byte[] valuesLen = serializeInteger(valueBytes.length);
			
			baos.write(valuesLen, 0, valuesLen.length);
			baos.write(valueBytes, 0, valueBytes.length);
		}

		byte[] values = baos.toByteArray();
		baos.reset();
		
		byte[] valuesLen = serializeInteger(values.length);
		baos.write(valuesLen, 0 , valuesLen.length);
		baos.write(values, 0, values.length);
		return values;
		
	}
	
	public static CQLType getCQLTypeForClass(Class c) {
		return compatTypes.get(c);
	}

}
