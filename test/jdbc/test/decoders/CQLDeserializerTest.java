package jdbc.test.decoders;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

import pl.dszczygiel.jdbc.nativeprotocol.constants.CQLType;
import pl.dszczygiel.jdbc.nativeprotocol.decoders.CQLDeserializer;
import pl.dszczygiel.jdbc.nativeprotocol.types.CQLListSetTypeMetadata;

public class CQLDeserializerTest {
	CQLDeserializer deserializer = new  CQLDeserializer();
	@Test
	public void deserializeInetTest() {
		byte[] inetBytes = new byte[] {4, 127, 0, 0, 1, 0, 0, 0x22, (byte) 0xB8};
		//CQLType
		//InetAddress addr = deserializer.deserialize(CQLType., value);
	}
	
	@Test
	public void deserializeListTest() {
		byte[] listBytes = new byte[] {0, 0, 0, 2,  0, 0, 0, 1, 'a',  0, 0, 0, 1, 'b'};
		
		Object o = deserializer.deserialize(new CQLListSetTypeMetadata(CQLType.LIST, CQLType.VARCHAR), listBytes);
		
		List<String> l = (List<String>) o;
		
		List<String> list = new ArrayList<String>();
		list.add("a");
		list.add("b");
		
		assertEquals(list, l);
	}
}
