package jdbc.test.decoders;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertNull;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.Test;

import pl.dszczygiel.jdbc.driver.exceptions.CQLException;
import pl.dszczygiel.jdbc.nativeprotocol.compression.NoCompression;
import pl.dszczygiel.jdbc.nativeprotocol.constants.CQLType;
import pl.dszczygiel.jdbc.nativeprotocol.constants.OpCode;
import pl.dszczygiel.jdbc.nativeprotocol.decoders.FrameDecoder;
import pl.dszczygiel.jdbc.nativeprotocol.frame.Frame;
import pl.dszczygiel.jdbc.nativeprotocol.frame.Header;
import pl.dszczygiel.jdbc.nativeprotocol.frame.HeaderFlags;
import pl.dszczygiel.jdbc.nativeprotocol.message.responses.ColumnSpecification;
import pl.dszczygiel.jdbc.nativeprotocol.message.responses.ResultKind;
import pl.dszczygiel.jdbc.nativeprotocol.message.responses.ResultMessage;
import pl.dszczygiel.jdbc.nativeprotocol.message.responses.Row;
import pl.dszczygiel.jdbc.nativeprotocol.types.CQLTypeMetadata;
import pl.dszczygiel.jdbc.utils.Converter;

public class FrameDecoderTest {
	FrameDecoder decoder = new FrameDecoder(new NoCompression());

	@Test
	public void headerDecoderTest() {
		Frame frame;
		byte[] hexFrame = new byte[] { (byte) 0x84, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x00 };

		frame = decoder.decode(hexFrame);
		Header header = frame.getHeader();
		HeaderFlags flags = new HeaderFlags();
		flags.setCompressionFlag(false);
		flags.setCustomPayloadFlag(false);
		flags.setTracingFlag(false);
		flags.setWarningFlag(false);
		assertEquals(flags, header.getFlags());
		assertEquals(0, header.getMessageLength());
		assertEquals(OpCode.READY, header.getOpCode());
		assertEquals(4, header.getProtocolVersion());
	}

	@Test
	public void rowsResultMessageTest() throws CQLException {
		String hexMessageString = "8400000b0800000089000000020000000100000003000c746573746b657973706163650004757365720007757365725f6964000900046e616d65000d000570686f6e65000e000000030000000400000001000000045061756c00000004069f6bc7000000040000000200000006536f72626574000000040d3ed78e0000000400000003000000034e69650000000413de4355";
		byte[] messageBytes = Converter.hexStringToByteArray(hexMessageString);

		Frame frame = decoder.decode(messageBytes);
		// Header
		Header header = frame.getHeader();
		HeaderFlags flags = new HeaderFlags();
		flags.setCompressionFlag(false);
		flags.setCustomPayloadFlag(false);
		flags.setTracingFlag(false);
		flags.setWarningFlag(false);

		assertEquals(flags, header.getFlags());
		assertEquals(137, header.getMessageLength());
		assertEquals(OpCode.RESULT, header.getOpCode());
		assertEquals(4, header.getProtocolVersion());
		assertEquals(11, header.getStreamID());

		// Message
		List<ColumnSpecification> specs = new ArrayList<ColumnSpecification>();
		ColumnSpecification cs = new ColumnSpecification();
		cs.setColumnName("user_id");
		cs.setColumnType(new CQLTypeMetadata(CQLType.INT));
		cs.setKeyspaceName("testkeyspace");
		cs.setTableName("user");
		specs.add(cs);
		cs = new ColumnSpecification();
		cs.setColumnName("name");
		cs.setColumnType(new CQLTypeMetadata(CQLType.VARCHAR));
		cs.setKeyspaceName("testkeyspace");
		cs.setTableName("user");
		specs.add(cs);
		cs = new ColumnSpecification();
		cs.setColumnName("phone");
		cs.setColumnType(new CQLTypeMetadata(CQLType.VARINT));
		cs.setKeyspaceName("testkeyspace");
		cs.setTableName("user");
		specs.add(cs);

		ResultMessage message = (ResultMessage) frame.getMessage();
		assertEquals(ResultKind.ROWS, message.getKind());
		assertEquals(3, message.getColumnCount());
		assertEquals(specs, message.getColumnSpecifications());
		Row row = message.getRows().get(0);
		assertEquals(1, row.getValueByIndex(0, specs));
		assertEquals("Paul", row.getValueByIndex(1, specs));
		assertEquals(new BigInteger("111111111"), row.getValueByIndex(2, specs));

		row = message.getRows().get(1);
		assertEquals(2, row.getValueByName("user_id", specs));
		assertEquals("Sorbet", row.getValueByName("name", specs));
		assertEquals(new BigInteger("222222222"), row.getValueByName("phone", specs));
	}

	
	@Test
	public void rowsResultMessageWithWarningTest() throws CQLException {
		String hexMessageString = "8408004708000000cc0001002c4167677265676174696f6e207175657279207573656420776974686f757420706172746974696f6e206b6579000000020000000100000003000c746573746b657973706163650004757365720007757365725f6964000900046e616d65000d000570686f6e65000e000000040000000400000001000000045061756c00000004069f6bc7000000040000000200000006536f72626574000000040d3ed78e0000000400000004000000034e6965ffffffff0000000400000003000000034e69650000000413de4355";
		byte[] messageBytes = Converter.hexStringToByteArray(hexMessageString);

		Frame frame = decoder.decode(messageBytes);
		// Header
		Header header = frame.getHeader();
		HeaderFlags flags = new HeaderFlags();
		flags.setCompressionFlag(false);
		flags.setCustomPayloadFlag(false);
		flags.setTracingFlag(false);
		flags.setWarningFlag(true);

		assertEquals(flags, header.getFlags());
		assertEquals(204, header.getMessageLength());
		assertEquals(OpCode.RESULT, header.getOpCode());
		assertEquals(4, header.getProtocolVersion());
		assertEquals(71, header.getStreamID());

		// Message
		List<ColumnSpecification> specs = new ArrayList<ColumnSpecification>();
		ColumnSpecification cs = new ColumnSpecification();
		cs.setColumnName("user_id");
		cs.setColumnType(new CQLTypeMetadata(CQLType.INT));
		cs.setKeyspaceName("testkeyspace");
		cs.setTableName("user");
		specs.add(cs);
		cs = new ColumnSpecification();
		cs.setColumnName("name");
		cs.setColumnType(new CQLTypeMetadata(CQLType.VARCHAR));
		cs.setKeyspaceName("testkeyspace");
		cs.setTableName("user");
		specs.add(cs);
		cs = new ColumnSpecification();
		cs.setColumnName("phone");
		cs.setColumnType(new CQLTypeMetadata(CQLType.VARINT));
		cs.setKeyspaceName("testkeyspace");
		cs.setTableName("user");
		specs.add(cs);

		ResultMessage message = (ResultMessage) frame.getMessage();
		assertEquals(ResultKind.ROWS, message.getKind());
		assertEquals(3, message.getColumnCount());
		assertEquals(specs, message.getColumnSpecifications());
		Row row = message.getRows().get(0);
		assertEquals(1, row.getValueByIndex(0, specs));
		assertEquals("Paul", row.getValueByIndex(1, specs));
		assertEquals(new BigInteger("111111111"), row.getValueByIndex(2, specs));

		row = message.getRows().get(1);
		assertEquals(2, row.getValueByName("user_id", specs));
		assertEquals("Sorbet", row.getValueByName("name", specs));
		assertEquals(new BigInteger("222222222"), row.getValueByName("phone", specs));
	}
	
	@Test
	public void preparedInsertResultMessageTest() throws CQLException {
		String resultHexString = "8400010008000000530000000400102f6061a4c3bc1f0c2228b4bc602286410000000100000002000000010000000c746573746b657973706163650004757365720007757365725f6964000900046e616d65000d0000000400000000";
		byte[] resultBytes = Converter.hexStringToByteArray(resultHexString);

		Frame frame = decoder.decode(resultBytes);
		// Header
		Header header = frame.getHeader();
		HeaderFlags flags = new HeaderFlags();
		flags.setCompressionFlag(false);
		flags.setCustomPayloadFlag(false);
		flags.setTracingFlag(false);
		flags.setWarningFlag(false);

		assertEquals(flags, header.getFlags());
		assertEquals(83, header.getMessageLength());
		assertEquals(OpCode.RESULT, header.getOpCode());
		assertEquals(4, header.getProtocolVersion());
		assertEquals(256, header.getStreamID());

		// Message
		List<ColumnSpecification> preparedSpecs = new ArrayList<ColumnSpecification>();
		ColumnSpecification cs = new ColumnSpecification();
		cs.setColumnName("user_id");
		cs.setColumnType(new CQLTypeMetadata(CQLType.INT));
		cs.setKeyspaceName("testkeyspace");
		cs.setTableName("user");
		preparedSpecs.add(cs);
		cs = new ColumnSpecification();
		cs.setColumnName("name");
		cs.setColumnType(new CQLTypeMetadata(CQLType.VARCHAR));
		cs.setKeyspaceName("testkeyspace");
		cs.setTableName("user");
		preparedSpecs.add(cs);
		cs = new ColumnSpecification();

		byte[] id = new byte[] {0x2f, 0x60, 0x61, (byte) 0xa4, (byte) 0xc3, (byte) 0xbc, 0x1f, 0x0c,
				0x22, 0x28, (byte) 0xb4, (byte) 0xbc, 0x60, 0x22, (byte) 0x86, 0x41};
		
		ResultMessage message = (ResultMessage) frame.getMessage();
		System.out.println();
		assertEquals(ResultKind.PREPARED, message.getKind());
		assertEquals(2, message.getPreparedStatementData().getColumnCount());
		assertEquals(preparedSpecs, message.getPreparedStatementData().getColumnSpecifications());
		assertArrayEquals(id, message.getPreparedStatementData().getId());
		assertEquals(1, message.getPreparedStatementData().getPrimaryKeyCount());
		assertEquals(new ArrayList<Integer>(Arrays.asList(0)), message.getPreparedStatementData().getPrimaryKeysIndexes());
		assert(message.getColumnSpecifications().isEmpty());
		assertNull(message.getRows());

	}
	
	@Test
	public void preparedSelectResultMessageTest() {
		String resultHexString = "840001000800000077000000040010327fa5f6c8301400878d569d1e87055c0000000100000001000000010000000c746573746b65797370616365000475736572000375696400090000000100000003000c746573746b657973706163650004757365720007757365725f6964000900046e616d65000d000570686f6e65000e";
		byte[] resultBytes = Converter.hexStringToByteArray(resultHexString);

		Frame frame = decoder.decode(resultBytes);
		// Header
		Header header = frame.getHeader();
		HeaderFlags flags = new HeaderFlags();
		flags.setCompressionFlag(false);
		flags.setCustomPayloadFlag(false);
		flags.setTracingFlag(false);
		flags.setWarningFlag(false);

		assertEquals(flags, header.getFlags());
		assertEquals(119, header.getMessageLength());
		assertEquals(OpCode.RESULT, header.getOpCode());
		assertEquals(4, header.getProtocolVersion());
		assertEquals(256, header.getStreamID());

		// Message
		List<ColumnSpecification> preparedSpecs = new ArrayList<ColumnSpecification>();
		ColumnSpecification cs = new ColumnSpecification();
		cs.setColumnName("uid");
		cs.setColumnType(new CQLTypeMetadata(CQLType.INT));
		cs.setKeyspaceName("testkeyspace");
		cs.setTableName("user");
		preparedSpecs.add(cs);

		byte[] id = new byte[] {0x32, 0x7f, (byte) 0xa5, (byte) 0xf6, (byte) 0xc8, (byte) 0x30, 0x14, 0x00,
				(byte) 0x87, (byte) 0x8d, (byte) 0x56, (byte) 0x9d, 0x1e, (byte) 0x87, (byte) 0x05, 0x5c};
		
		List<ColumnSpecification> specs = new ArrayList<ColumnSpecification>();
		cs = new ColumnSpecification();
		cs.setColumnName("user_id");
		cs.setColumnType(new CQLTypeMetadata(CQLType.INT));
		cs.setKeyspaceName("testkeyspace");
		cs.setTableName("user");
		specs.add(cs);
		cs = new ColumnSpecification();
		cs.setColumnName("name");
		cs.setColumnType(new CQLTypeMetadata(CQLType.VARCHAR));
		cs.setKeyspaceName("testkeyspace");
		cs.setTableName("user");
		specs.add(cs);
		cs = new ColumnSpecification();
		cs.setColumnName("phone");
		cs.setColumnType(new CQLTypeMetadata(CQLType.VARINT));
		cs.setKeyspaceName("testkeyspace");
		cs.setTableName("user");
		specs.add(cs);
		
		ResultMessage message = (ResultMessage) frame.getMessage();
		System.out.println();
		assertEquals(ResultKind.PREPARED, message.getKind());
		assertEquals(1, message.getPreparedStatementData().getColumnCount());
		assertEquals(preparedSpecs, message.getPreparedStatementData().getColumnSpecifications());
		assertArrayEquals(id, message.getPreparedStatementData().getId());
		assertEquals(1, message.getPreparedStatementData().getPrimaryKeyCount());
		assertEquals(new ArrayList<Integer>(Arrays.asList(0)), message.getPreparedStatementData().getPrimaryKeysIndexes());
		assertEquals(specs, message.getColumnSpecifications());
		
		assertNull(message.getRows());
	}
}
