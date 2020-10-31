package jdbc.test.decoders;

import static org.junit.Assert.assertNull;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

import pl.dszczygiel.jdbc.nativeprotocol.constants.CQLType;
import pl.dszczygiel.jdbc.nativeprotocol.constants.OpCode;
import pl.dszczygiel.jdbc.nativeprotocol.decoders.FrameLayoutDecoders;
import pl.dszczygiel.jdbc.nativeprotocol.types.CQLListSetTypeMetadata;
import pl.dszczygiel.jdbc.nativeprotocol.types.CQLMapTypeMetadata;
import pl.dszczygiel.jdbc.nativeprotocol.types.CQLTypeMetadata;

public class FrameLayoutDecodersTest {
	FrameLayoutDecoders decoders = new FrameLayoutDecoders();

	@Test
	public void decodeIntTest() {
		byte[] intBytes = {0x00, 0x00, 0x00, 0x09};
		assertEquals(9, decoders.decodeInt(intBytes, 0, intBytes.length));
	}
	
	@Test
	public void decodeOpcCodeTest() {
		assertEquals(OpCode.ERROR, OpCode.getOpCode(0));
		assertEquals(OpCode.READY, OpCode.getOpCode(2));
		assertEquals(OpCode.EXECUTE, OpCode.getOpCode(10));
	}
	
	@Test
	public void decodeStringTest() {
		String s1 = "qwerty";
		String result = decoders.decodeString(s1.getBytes(), 0, s1.length(), StandardCharsets.UTF_8);
		
		assertEquals(s1, result);
		result = decoders.decodeString(s1.getBytes(), 0, 0, StandardCharsets.UTF_8);
		assertEquals("", result);
		result = decoders.decodeString(s1.getBytes(), 0, -1, StandardCharsets.UTF_8);
		assertNull(result);
	}
	
	@Test
	public void decodeStringListTest() {
		List<String> list = new ArrayList<String>();
		list.add("a");
		list.add("ab");
		
		List<String> decodedList = new ArrayList<String>();
		byte[] listBytes = new byte[] {0x00, 0x02,  0x00, 0x01, 'a',  0x00, 0x02, 'a', 'b'};
		int len = decoders.decodeStringList(listBytes, 0, decodedList, StandardCharsets.UTF_8);
		System.out.println(len);
		for(String s : decodedList) {
			System.out.println(s);
		}
		
	}
	
	@Test
	public void decodeInetTest() throws UnknownHostException {
		byte[] inetBytes = new byte[] {4, 127, 0, 0, 1, 0, 0, 0x22, (byte) 0xB8};
		InetAddress inet = InetAddress.getByName("127.0.0.1");
		InetAddress inetDecoded = decoders.decodeInet(inetBytes, 0);
		assertEquals(inet, inetDecoded);
	}
	
	@Test
	public void decodeCqlTypeTest() {
		CQLTypeMetadata simpleType = new CQLTypeMetadata(CQLType.INT);
		byte[] intBytes = new byte[] {0, 9};
		CQLTypeMetadata decoded = decoders.decodeCqlType(intBytes, 0);
		assertEquals(2, decoded.getSize());
		assertEquals(simpleType.getType(), decoded.getType());
		
		CQLListSetTypeMetadata listType = new CQLListSetTypeMetadata(CQLType.LIST, CQLType.VARCHAR);
		byte[] listBytes = new byte[] {0, 32, 0, 13};
		decoded = decoders.decodeCqlType(listBytes, 0);
		assertEquals(4, decoded.getSize());
		assertEquals(listType.getType(), decoded.getType());
		assertEquals(listType.getElementsType(), ((CQLListSetTypeMetadata)decoded).getElementsType());
		
		CQLMapTypeMetadata mapType = new CQLMapTypeMetadata(CQLType.MAP, CQLType.VARCHAR, CQLType.INT);
		byte[] mapBytes = new byte[] {0, 0x21, 0, 13, 0, 9};
		decoded = decoders.decodeCqlType(mapBytes, 0);
		assertEquals(6, decoded.getSize());
		assertEquals(mapType.getType(), decoded.getType());
		assertEquals(mapType.getKeyType(), ((CQLMapTypeMetadata)decoded).getKeyType());
		assertEquals(mapType.getValueType(), ((CQLMapTypeMetadata)decoded).getValueType());


	}

}
