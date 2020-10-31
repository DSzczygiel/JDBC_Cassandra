package pl.dszczygiel.jdbc.nativeprotocol.encoders;

import java.io.ByteArrayOutputStream;
import java.util.Map;

import pl.dszczygiel.jdbc.nativeprotocol.compression.Compression;
import pl.dszczygiel.jdbc.nativeprotocol.encoders.framelayout.FrameLayoutEncoders;
import pl.dszczygiel.jdbc.nativeprotocol.encoders.framelayout.ValueType;
import pl.dszczygiel.jdbc.nativeprotocol.frame.Message;
import pl.dszczygiel.jdbc.nativeprotocol.messages.requests.QueryFlags;
import pl.dszczygiel.jdbc.nativeprotocol.messages.requests.QueryMessage;
import pl.dszczygiel.jdbc.nativeprotocol.messages.requests.QueryMessage.QueryMessageType;

public class QueryMessageEncoder extends MessageEncoder {
	FrameLayoutEncoders layoutCodecs = new FrameLayoutEncoders();

	@Override
	public byte[] encode(Message message) {
		QueryMessage qm = (QueryMessage) message;
		ByteArrayOutputStream baos = new ByteArrayOutputStream();

		if (qm.getMessageType() == QueryMessageType.QUERY) {
			byte[] queryLongString = layoutCodecs.encodeLongString(qm.getQuery());
			baos.write(queryLongString, 0, queryLongString.length);
		}else {
			byte[] idBytes = layoutCodecs.encodeShortBytes(qm.getQueryId());
			baos.write(idBytes, 0 , idBytes.length);
		}
		
		byte[] queryConsistency = layoutCodecs.encodeConsistency(qm.getConsistency());
		baos.write(queryConsistency, 0, queryConsistency.length);

		QueryFlags flags = qm.getQueryFlags();
		byte[] flagsBytes = encodeQueryFlags(qm.getQueryFlags());
		baos.write(flagsBytes, 0, flagsBytes.length);

		byte[] valuesBytes;
		if (flags.getValuesFlag() && flags.getNamesForValuesFlag()) {
			valuesBytes = encodeNamedValues(qm.getNamedValues());
			baos.write(valuesBytes, 0, valuesBytes.length);
		} else if (flags.getValuesFlag()) {
			valuesBytes = encodeValues(qm.getValues());
			baos.write(valuesBytes, 0, valuesBytes.length);
		}

		if (flags.getPageSizeFlag()) {
			byte[] resultPageSize = layoutCodecs.encodeInt(qm.getPageSize());
			baos.write(resultPageSize, 0, resultPageSize.length);
		}

		if (flags.getPagingStateFlag()) {
			byte[] pagingState = layoutCodecs.encodeBytes(qm.getPagingState().getValue());
			baos.write(pagingState, 0, pagingState.length);
		}

		if (flags.getDefaultTimestampFlag()) {
			byte[] timestamp = layoutCodecs.encodeLong(qm.getTimestamp());
			baos.write(timestamp, 0, timestamp.length);
		}

		return baos.toByteArray();
	}
	
	byte[] encodeValues(Map<Integer, ValueType> values) {
		ByteArrayOutputStream mBaos = new ByteArrayOutputStream();
		byte[] nrOfValues = layoutCodecs.encodeShort((short) values.size());
		mBaos.write(nrOfValues, 0, nrOfValues.length);
		for (Map.Entry<Integer, ValueType> entry : values.entrySet()) {
			byte[] value = layoutCodecs.encodeValue(entry.getValue());
			mBaos.write(value, 0, value.length);
		}
		return mBaos.toByteArray();
	}
	
	byte[] encodeNamedValues(Map<String, ValueType> namedValues) {
		ByteArrayOutputStream mBaos = new ByteArrayOutputStream();

		byte[] nrOfValues = layoutCodecs.encodeShort((short) namedValues.size());
		mBaos.write(nrOfValues, 0, nrOfValues.length);
		for (Map.Entry<String, ValueType> entry : namedValues.entrySet()) {
			byte[] name = layoutCodecs.encodeString(entry.getKey());
			byte[] value = layoutCodecs.encodeValue(entry.getValue());
			mBaos.write(name, 0, name.length);
			mBaos.write(value, 0, value.length);
		}
		return mBaos.toByteArray();
	}
	
	byte[] encodeQueryFlags(QueryFlags flags) {
		ByteArrayOutputStream mBaos = new ByteArrayOutputStream();

		byte flagsByte = 0;
		if (flags.getValuesFlag())
			flagsByte |= 1;
		if (flags.getSkipMetadataFlag())
			flagsByte |= 2;
		if (flags.getPageSizeFlag())
			flagsByte |= 4;
		if (flags.getPagingStateFlag())
			flagsByte |= 8;
		if (flags.getSerialConsistencyFlag())
			flagsByte |= 16;
		if (flags.getDefaultTimestampFlag())
			flagsByte |= 32;
		if (flags.getNamesForValuesFlag())
			flagsByte |= 64;
		mBaos.write(flagsByte);
		
		return mBaos.toByteArray();
	}

}
