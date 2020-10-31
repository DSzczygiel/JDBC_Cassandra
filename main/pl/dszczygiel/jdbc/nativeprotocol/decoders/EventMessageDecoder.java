package pl.dszczygiel.jdbc.nativeprotocol.decoders;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;

import pl.dszczygiel.jdbc.nativeprotocol.compression.Compression;
import pl.dszczygiel.jdbc.nativeprotocol.constants.EventType;
import pl.dszczygiel.jdbc.nativeprotocol.frame.Header;
import pl.dszczygiel.jdbc.nativeprotocol.frame.Message;
import pl.dszczygiel.jdbc.nativeprotocol.message.responses.EventMessage;
import pl.dszczygiel.jdbc.nativeprotocol.message.responses.SchemaChangeData;
import pl.dszczygiel.jdbc.nativeprotocol.message.responses.StatusChangeData;
import pl.dszczygiel.jdbc.nativeprotocol.message.responses.TopologyChangeData;

public class EventMessageDecoder extends MessageDecoder {
	private int currentPosition = 0;
	private EventMessage em = new EventMessage();
	private FrameLayoutDecoders decoders = new FrameLayoutDecoders();

	public EventMessageDecoder(boolean readWarnings) {
		super(readWarnings);
	}

	@Override
	public Message decode(byte[] value) {
		int strLen = decoders.decodeShort(value, currentPosition);
		currentPosition += 2;
		String eventTypeStr = decoders.decodeString(value, currentPosition, strLen, StandardCharsets.UTF_8);
		currentPosition += strLen;
		EventType eventType = EventType.getByName(eventTypeStr);
		em.setEventType(eventType);

		switch (eventType) {
		case TOPOLOGY_CHANGE: {
			decodeTopologyChange(value);
			break;
		}
		case STATUS_CHANGE: {
			decodeStatusChange(value);
			break;
		}
		case SCHEMA_CHANGE: {
			decodeSchemaChange(value);
			break;
		}
		}
		return em;
	}

	private void decodeTopologyChange(byte[] value) {
		TopologyChangeData topologyChangeData = new TopologyChangeData();
		int strLen = decoders.decodeShort(value, currentPosition);
		currentPosition += 2;
		String changeTypeStr = decoders.decodeString(value, currentPosition, strLen, StandardCharsets.UTF_8);
		currentPosition += strLen;
		try {
			InetAddress inet = decoders.decodeInet(value, currentPosition);
			currentPosition += inet.getAddress().length + 4;
			topologyChangeData.setAddress(inet);
			topologyChangeData.setChangeType(changeTypeStr);
			em.setTopologyChangeData(topologyChangeData);
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private void decodeStatusChange(byte[] value) {
		StatusChangeData statusChangeData = new StatusChangeData();
		int strLen = decoders.decodeShort(value, currentPosition);
		currentPosition += 2;
		String changeTypeStr = decoders.decodeString(value, currentPosition, strLen, StandardCharsets.UTF_8);
		currentPosition += strLen;
		try {
			InetAddress inet = decoders.decodeInet(value, currentPosition);
			currentPosition += inet.getAddress().length + 4;
			statusChangeData.setAddress(inet);
			statusChangeData.setChangeType(changeTypeStr);
			em.setStatusChangeData(statusChangeData);
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private void decodeSchemaChange(byte[] value) {
		SchemaChangeData schemaChangeData = new SchemaChangeData();
		int strLen = decoders.decodeShort(value, currentPosition);
		currentPosition += 2;
		String changeTypeStr = decoders.decodeString(value, currentPosition, strLen, StandardCharsets.UTF_8);
		currentPosition += strLen;
		schemaChangeData.setChangeType(changeTypeStr);
		strLen = decoders.decodeShort(value, currentPosition);
		currentPosition += 2;
		String changeTargetStr = decoders.decodeString(value, currentPosition, strLen, StandardCharsets.UTF_8);
		currentPosition += strLen;

		switch (changeTargetStr) {
		case SchemaChangeData.TARGET_KEYSPACE: {
			strLen = decoders.decodeShort(value, currentPosition);
			currentPosition += 2;
			String keyspaceStr = decoders.decodeString(value, currentPosition, strLen, StandardCharsets.UTF_8);
			currentPosition += strLen;
			schemaChangeData.setAffectedKeyspace(keyspaceStr);
			em.setSchemaChangeData(schemaChangeData);
			break;
		}
		case SchemaChangeData.TARGET_TABLE: {
			strLen = decoders.decodeShort(value, currentPosition);
			currentPosition += 2;
			String keyspaceStr = decoders.decodeString(value, currentPosition, strLen, StandardCharsets.UTF_8);
			currentPosition += strLen;
			strLen = decoders.decodeShort(value, currentPosition);
			currentPosition += 2;
			String tableStr = decoders.decodeString(value, currentPosition, strLen, StandardCharsets.UTF_8);
			currentPosition += strLen;
			schemaChangeData.setAffectedKeyspace(keyspaceStr);
			schemaChangeData.setAffectedTable(tableStr);
			em.setSchemaChangeData(schemaChangeData);
			break;
		}

		}

	}

}
