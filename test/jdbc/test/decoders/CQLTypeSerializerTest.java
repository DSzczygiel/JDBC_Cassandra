package jdbc.test.decoders;

import static org.junit.Assert.assertArrayEquals;

import java.io.ByteArrayOutputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.sql.Date;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

import pl.dszczygiel.jdbc.driver.exceptions.CQLSerializerException;
import pl.dszczygiel.jdbc.nativeprotocol.constants.CQLType;
import pl.dszczygiel.jdbc.nativeprotocol.encoders.framelayout.CQLList;
import pl.dszczygiel.jdbc.nativeprotocol.encoders.framelayout.CQLSerializer;
import pl.dszczygiel.jdbc.nativeprotocol.encoders.framelayout.ValueType;
import pl.dszczygiel.jdbc.nativeprotocol.types.CQLListSetTypeMetadata;
import pl.dszczygiel.jdbc.nativeprotocol.types.CQLTypeMetadata;
import pl.dszczygiel.jdbc.nativeprotocol.types.CQLUdtMetadata;
import pl.dszczygiel.jdbc.nativeprotocol.types.UserDefinedType;

public class CQLTypeSerializerTest {
	CQLSerializer serializer = new  CQLSerializer();
	@Test
	public void serializeIntegerTest() throws CQLSerializerException {
		int val = 663733;
		byte[] bytes = ByteBuffer.allocate(4).putInt(val).array();	
		
		byte[] serialized = serializer.serialize(new CQLTypeMetadata(CQLType.INT), val);
		assertArrayEquals(bytes, serialized);
		}
	
	@Test
	public void serializeLongTest() throws CQLSerializerException {
		long val = 434366355733l;
		byte[] bytes = ByteBuffer.allocate(8).putLong(val).array();	
		
		byte[] serialized = serializer.serialize(new CQLTypeMetadata(CQLType.BIGINT), val);
		assertArrayEquals(bytes, serialized);
	}
	
	@Test
	public void serializeDoubleTest() throws CQLSerializerException {
		double val = 24532.344;
		byte[] bytes = ByteBuffer.allocate(8).putDouble(val).array();	
		
		byte[] serialized = serializer.serialize(new CQLTypeMetadata(CQLType.DOUBLE), val);
		assertArrayEquals(bytes, serialized);
	}
	
	@Test
	public void serializeShortTest() throws CQLSerializerException {
		short val = 10424;
		byte[] bytes = ByteBuffer.allocate(2).putShort(val).array();	
		
		byte[] serialized = serializer.serialize(new CQLTypeMetadata(CQLType.SMALLINT), val);
		assertArrayEquals(bytes, serialized);
	}
	
	@Test
	public void serializeByteTest() throws CQLSerializerException {
		byte val = (byte) 231;
		byte[] bytes = new byte[1];
		bytes[0] = val;
		
		byte[] serialized = serializer.serialize(new CQLTypeMetadata(CQLType.TINYINT), val);
		assertArrayEquals(bytes, serialized);
	}
	
	@Test
	public void serializeBooleanTest() throws CQLSerializerException {
		boolean val = true;
		byte[] bytes = new byte[1];
		bytes[0] = 1;
		
		byte[] serialized = serializer.serialize(new CQLTypeMetadata(CQLType.BOOLEAN), val);
		assertArrayEquals(bytes, serialized);
	}
	
	@Test
	public void serializeFloatTest() throws CQLSerializerException {
		float val = 24532.34f;
		byte[] bytes = ByteBuffer.allocate(4).putFloat(val).array();	
		
		byte[] serialized = serializer.serialize(new CQLTypeMetadata(CQLType.FLOAT), val);
		assertArrayEquals(bytes, serialized);
	}
	
	@Test
	public void serializeStringTest() throws CQLSerializerException {
		String val = "fsssafaąłówafałąźż";
		byte[] bytes = val.getBytes();	
		
		byte[] serialized = serializer.serialize(new CQLTypeMetadata(CQLType.VARCHAR), val);
		assertArrayEquals(bytes, serialized);
	}
	
	@Test
	public void serializeDateTest() throws CQLSerializerException {
		Date date = new Date(1587912456);
		int dateInt = (int) date.getTime();
		byte[] bytes = ByteBuffer.allocate(4).putInt(dateInt).array();	
		
		byte[] serialized = serializer.serialize(new CQLTypeMetadata(CQLType.DATE), date);
		assertArrayEquals(bytes, serialized);
	}
	
	@Test
	public void serializeAsciiStringTest() throws CQLSerializerException {
		String val = "Yihfasiosfjishiosaghihasisghoisghgsdsdg";
		byte[] bytes = val.getBytes();	
		
		byte[] serialized = serializer.serialize(new CQLTypeMetadata(CQLType.ASCII), val);
		assertArrayEquals(bytes, serialized);
	}
	
	@Test
	public void serializeBytesTest() throws CQLSerializerException {
		String val = "Yihfasiosfjishiosaghihasisghoisghgsdsdg";
		byte[] bytes = val.getBytes();	
		
		byte[] serialized = serializer.serialize(new CQLTypeMetadata(CQLType.BLOB), bytes);
		assertArrayEquals(bytes, serialized);
	}
	
	@Test
	public void serializeBigIntegerTest() throws CQLSerializerException {
		BigInteger bi = new BigInteger("1234567890");
		byte[] bytes = bi.toByteArray();	
		
		byte[] serialized = serializer.serialize(new CQLTypeMetadata(CQLType.VARINT), bi);
		assertArrayEquals(bytes, serialized);
	}
	
	@Test
	public void serializeBigDecimalTest() throws CQLSerializerException {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		BigInteger bi = new BigInteger("1234567890");
		int scale = 5;
		BigDecimal bd = new BigDecimal(bi, scale);
		
		baos.write(ByteBuffer.allocate(4).putInt(scale).array(), 0, 4);
		baos.write(bi.toByteArray(), 0, bi.toByteArray().length);
		byte[] bytes = baos.toByteArray();	
		
		byte[] serialized = serializer.serialize(new CQLTypeMetadata(CQLType.DECIMAL), bd);
		assertArrayEquals(bytes, serialized);
	}
	
	@Test
	public void serializeListTest() throws CQLSerializerException {
		List<Integer> intList = new ArrayList<Integer>();	
		intList.add(2);
		intList.add(5);
		
		List<String> stringList = new ArrayList<String>();
		stringList.add("a");
		stringList.add("b");
		
		
		byte[] intListBytes = new byte[] {0, 0, 0, 2,  0, 0, 0, 4,  0, 0, 0, 2,  0, 0, 0, 4, 0, 0, 0, 5};
		byte[] strListBytes = new byte[] {0, 0, 0, 2,  0, 0, 0, 1,  97,  0, 0, 0, 1,  98};
		
		CQLListSetTypeMetadata cqlIntList = new CQLListSetTypeMetadata(CQLType.LIST, CQLType.INT);
		CQLListSetTypeMetadata cqlStrList = new CQLListSetTypeMetadata(CQLType.LIST, CQLType.VARCHAR);
		
		byte[] serialized = serializer.serialize(cqlIntList, intList);
		byte[] serializedStr = serializer.serialize(cqlStrList, stringList);
		assertArrayEquals(intListBytes, serialized);
		assertArrayEquals(strListBytes, serializedStr);
	}
	
	@Test
	public void serializeUdtTest() throws SQLException, CQLSerializerException {
		byte[] udtBytes = new byte[] { 0,0,0,4,0,0,0,(byte) 0xB4, 0,0,0,4,0,0,0,0x1E, 0,0,0,4,0x6D,0x61,0x6C,0x65};
		List<String> fieldNames = new ArrayList<String>();
		fieldNames.add("height");
		fieldNames.add("age");
		fieldNames.add("gender");
		
		List<CQLTypeMetadata> fieldTypes = new ArrayList<CQLTypeMetadata>();
		fieldTypes.add(new CQLTypeMetadata(CQLType.INT));
		fieldTypes.add(new CQLTypeMetadata(CQLType.INT));
		fieldTypes.add(new CQLTypeMetadata(CQLType.VARCHAR));
		
		CQLUdtMetadata meta = new CQLUdtMetadata(CQLType.UDT, "jdbckeyspace", "user_info", fieldNames, fieldTypes);
		UserDefinedType udt = new UserDefinedType(meta);
		udt.setInt("height", 180);
		udt.setInt("age", 30);
		udt.setString("gender", "male");
		
		byte[] serialized = serializer.serialize(meta, udt);
		assertArrayEquals(udtBytes, serialized);
	}
}
