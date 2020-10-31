package pl.dszczygiel.jdbc.nativeprotocol.encoders;

import java.io.ByteArrayOutputStream;

import pl.dszczygiel.jdbc.nativeprotocol.encoders.framelayout.FrameLayoutEncoders;
import pl.dszczygiel.jdbc.nativeprotocol.frame.Message;
import pl.dszczygiel.jdbc.nativeprotocol.messages.requests.BatchMessage;
import pl.dszczygiel.jdbc.nativeprotocol.messages.requests.QueryFlags;
import pl.dszczygiel.jdbc.nativeprotocol.messages.requests.QueryMessage;
import pl.dszczygiel.jdbc.nativeprotocol.messages.requests.QueryMessage.QueryMessageType;

public class BatchMessageEncoder extends MessageEncoder{
	FrameLayoutEncoders encoders = new FrameLayoutEncoders();
	QueryMessageEncoder queryMessageEncoder = new QueryMessageEncoder();
	
	@Override
	public byte[] encode(Message value) {		
		BatchMessage bm = (BatchMessage) value;
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		
		//batch type
		byte batchType = (byte) bm.getBatchType().ordinal();
		baos.write(batchType);
		//queries count
		byte[] queriesCountBytes = encoders.encodeShort((short)bm.getQueriesCount());
		baos.write(queriesCountBytes, 0, queriesCountBytes.length);
		
		for(QueryMessage qm : bm.getQueries()) {
			byte[] queryBytes = encodeQuery(qm);
			baos.write(queryBytes, 0, queryBytes.length);
		}

		byte[] consistencyBytes = encoders.encodeConsistency(bm.getConsistency());
		baos.write(consistencyBytes, 0, consistencyBytes.length);
		
		QueryFlags qf = bm.getFlags();
		qf.setNamesForValuesFlag(false);
		qf.setPageSizeFlag(false);
		qf.setSkipMetadataFlag(false);
		qf.setPagingStateFlag(false);
		
		byte[] flagsBytes = queryMessageEncoder.encodeQueryFlags(qf);
		baos.write(flagsBytes, 0, flagsBytes.length);
		
		if(qf.getSerialConsistencyFlag()) {
			byte[] serialConsistencyBytes = encoders.encodeConsistency(bm.getSerialConsistency());
			baos.write(serialConsistencyBytes, 0, serialConsistencyBytes.length);	
		}
		
		if(qf.getDefaultTimestampFlag()) {
			byte[] timestampBytes = encoders.encodeLong(bm.getTimestamp());
			baos.write(timestampBytes, 0, timestampBytes.length);
		}
		
		return baos.toByteArray();
	}

	//TODO not support named values
	private byte[] encodeQuery(QueryMessage message) {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		//kind
		byte kind = (byte) (message.getMessageType() == QueryMessageType.QUERY? 0 : 1);
		baos.write(kind);
		
		if(message.getMessageType() == QueryMessageType.QUERY) {
			byte[] query = encoders.encodeLongString(message.getQuery());
			baos.write(query, 0, query.length);
		}else {
			byte[] queryId = encoders.encodeShortBytes(message.getQueryId());
			baos.write(queryId, 0, queryId.length);
		}
		
			byte[] values = queryMessageEncoder.encodeValues(message.getValues());
			baos.write(values, 0, values.length);

		return baos.toByteArray();
		
	}
}
