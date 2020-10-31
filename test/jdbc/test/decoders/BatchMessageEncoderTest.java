package jdbc.test.decoders;


import static org.junit.jupiter.api.Assertions.assertArrayEquals;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;

import org.junit.jupiter.api.Test;

import pl.dszczygiel.jdbc.nativeprotocol.constants.BatchType;
import pl.dszczygiel.jdbc.nativeprotocol.constants.CQLType;
import pl.dszczygiel.jdbc.nativeprotocol.constants.Consistency;
import pl.dszczygiel.jdbc.nativeprotocol.encoders.BatchMessageEncoder;
import pl.dszczygiel.jdbc.nativeprotocol.encoders.framelayout.ValueType;
import pl.dszczygiel.jdbc.nativeprotocol.messages.requests.BatchMessage;
import pl.dszczygiel.jdbc.nativeprotocol.messages.requests.QueryFlags;
import pl.dszczygiel.jdbc.nativeprotocol.messages.requests.QueryMessage;
import pl.dszczygiel.jdbc.nativeprotocol.messages.requests.QueryMessage.QueryMessageType;
import pl.dszczygiel.jdbc.nativeprotocol.types.CQLTypeMetadata;
import pl.dszczygiel.jdbc.utils.Converter;

public class BatchMessageEncoderTest {
	@Test
	public void encodeBatchWithSimpleStatements() {
		BatchMessage batchMessage = new BatchMessage();
		BatchMessageEncoder batchMessageEncoder = new BatchMessageEncoder();
		String batchBytesString = "0100030000000040696e7365727420696e746f20757365722028757365725f69642c206e616d652c2070686f6e65292056414c5545532028362c20276464272c203636363636362900000000000042696e7365727420696e746f20757365722028757365725f69642c206e616d652c2070686f6e65292056414c5545532028372c20276468686864272c2037373636362900000000000047696e7365727420696e746f20757365722028757365725f69642c206e616d652c2070686f6e65292056414c5545532028382c20277373736464272c2038383838363636363636290000000a200005ad9ce7701cc8";
		byte[] batchBytes = Converter.hexStringToByteArray(batchBytesString);

		QueryFlags qf = new QueryFlags();
		List<QueryMessage> qmList = new ArrayList<QueryMessage>();
		qf.setDefaultTimestampFlag(true);
		QueryMessage qm1 = new QueryMessage();
		qm1.setQuery("insert into user (user_id, name, phone) VALUES (6, 'dd', 666666)");
		qm1.setMessageType(QueryMessageType.QUERY);
		QueryMessage qm2 = new QueryMessage();
		qm2.setQuery("insert into user (user_id, name, phone) VALUES (7, 'dhhhd', 77666)");
		qm2.setMessageType(QueryMessageType.QUERY);
		QueryMessage qm3 = new QueryMessage();
		qm3.setQuery("insert into user (user_id, name, phone) VALUES (8, 'sssdd', 8888666666)");
		qm3.setMessageType(QueryMessageType.QUERY);
		qmList.add(qm1);
		qmList.add(qm2);
		qmList.add(qm3);
		batchMessage.setBatchType(BatchType.UNLOGGED);
		batchMessage.setConsistency(Consistency.LOCAL_ONE);
		batchMessage.setQueriesCount(3);
		batchMessage.setFlags(qf);
		batchMessage.setQueries(qmList);
		long time = 1598264292941000l;
		batchMessage.setTimestamp(time);

		byte[] encodedMessage = batchMessageEncoder.encode(batchMessage);
		assertArrayEquals(batchBytes, encodedMessage);
	}

	@Test
	public void encodeBatchWithPreparedStatements() {
		BatchMessage batchMessage = new BatchMessage();
		BatchMessageEncoder batchMessageEncoder = new BatchMessageEncoder();
		String batchBytesString = "0100020100107396c43108a8c85153880c77b2bb505b0003000000040000000b000000044b6b4c6b000000030ca7f0010010b53f6f41d680e25d14e8bcd37ec6c0090002000000045061756c0000000400000001000a200005ad9de69217b0";
		byte[] batchBytes = Converter.hexStringToByteArray(batchBytesString);

		QueryFlags qf = new QueryFlags();
		List<QueryMessage> qmList = new ArrayList<QueryMessage>();
		qf.setDefaultTimestampFlag(true);
		QueryMessage qm1 = new QueryMessage();
		qm1.setQueryId(Converter.hexStringToByteArray("7396c43108a8c85153880c77b2bb505b"));
		TreeMap<Integer, ValueType> values = new TreeMap<Integer, ValueType>();
		values.put(0, new ValueType(new CQLTypeMetadata(CQLType.INT), 11));
		values.put(1, new ValueType(new CQLTypeMetadata(CQLType.VARCHAR), "KkLk"));
		values.put(2, new ValueType(new CQLTypeMetadata(CQLType.VARINT), new BigInteger("829424")));
		qm1.setValues(values);
		qm1.setMessageType(QueryMessageType.EXECUTE);
		QueryMessage qm2 = new QueryMessage();
		qm2.setQueryId(Converter.hexStringToByteArray("b53f6f41d680e25d14e8bcd37ec6c009"));
		qm2.setMessageType(QueryMessageType.EXECUTE);
		TreeMap<Integer, ValueType> values2 = new TreeMap<Integer, ValueType>();
		values2.put(0, new ValueType(new CQLTypeMetadata(CQLType.VARCHAR), "Paul"));
		values2.put(1, new ValueType(new CQLTypeMetadata(CQLType.INT), 1));
		qm2.setValues(values2);

		qmList.add(qm1);
		qmList.add(qm2);
		batchMessage.setBatchType(BatchType.UNLOGGED);
		batchMessage.setConsistency(Consistency.LOCAL_ONE);
		batchMessage.setQueriesCount(2);
		batchMessage.setFlags(qf);
		batchMessage.setQueries(qmList);
		long time = 1598268573358000l;
		batchMessage.setTimestamp(time);

		byte[] encodedMessage = batchMessageEncoder.encode(batchMessage);
		assertArrayEquals(batchBytes, encodedMessage);
	}

	@Test
	public void batchTestWithMixedStatements() {
		BatchMessage batchMessage = new BatchMessage();
		BatchMessageEncoder batchMessageEncoder = new BatchMessageEncoder();
		String batchBytesString = "0100020000000040696e7365727420696e746f20757365722028757365725f69642c206e616d652c2070686f6e65292056414c5545532028362c20276464272c203636363636362900000100107396c43108a8c85153880c77b2bb505b0003000000040000000b000000044b6b4c6b000000030ca7f0000a200005ad9e88bf4a08";
		byte[] batchBytes = Converter.hexStringToByteArray(batchBytesString);

		QueryFlags qf = new QueryFlags();
		List<QueryMessage> qmList = new ArrayList<QueryMessage>();
		qf.setDefaultTimestampFlag(true);
		QueryMessage qm1 = new QueryMessage();
		qm1.setQuery("insert into user (user_id, name, phone) VALUES (6, 'dd', 666666)");
		qm1.setMessageType(QueryMessageType.QUERY);
		QueryMessage qm2 = new QueryMessage();
		qm2.setQueryId(Converter.hexStringToByteArray("7396c43108a8c85153880c77b2bb505b"));
		qm2.setMessageType(QueryMessageType.EXECUTE);
		TreeMap<Integer, ValueType> values = new TreeMap<Integer, ValueType>();
		values.put(0, new ValueType(new CQLTypeMetadata(CQLType.INT), 11));
		values.put(1, new ValueType(new CQLTypeMetadata(CQLType.VARCHAR), "KkLk"));
		values.put(2, new ValueType(new CQLTypeMetadata(CQLType.VARINT), new BigInteger("829424")));
		qm2.setValues(values);

		qmList.add(qm1);
		qmList.add(qm2);
		batchMessage.setBatchType(BatchType.UNLOGGED);
		batchMessage.setConsistency(Consistency.LOCAL_ONE);
		batchMessage.setQueriesCount(2);
		batchMessage.setFlags(qf);
		batchMessage.setQueries(qmList);
		long time = 1598271294229000l;
		batchMessage.setTimestamp(time);

		byte[] encodedMessage = batchMessageEncoder.encode(batchMessage);
		System.out.println();
		for (byte b : batchBytes) {
			System.out.print(String.format("%02x ", b));
		}
		System.out.println();
		for (byte b : encodedMessage) {
			System.out.print(String.format("%02x ", b));
		}
		assertArrayEquals(batchBytes, encodedMessage);

	}
}
