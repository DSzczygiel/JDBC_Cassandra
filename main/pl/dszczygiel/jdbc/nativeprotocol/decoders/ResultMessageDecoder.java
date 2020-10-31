package pl.dszczygiel.jdbc.nativeprotocol.decoders;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import pl.dszczygiel.jdbc.driver.PagingState;
import pl.dszczygiel.jdbc.nativeprotocol.compression.Compression;
import pl.dszczygiel.jdbc.nativeprotocol.constants.CQLType;
import pl.dszczygiel.jdbc.nativeprotocol.frame.Header;
import pl.dszczygiel.jdbc.nativeprotocol.frame.Message;
import pl.dszczygiel.jdbc.nativeprotocol.message.responses.ColumnSpecification;
import pl.dszczygiel.jdbc.nativeprotocol.message.responses.PreparedStatementData;
import pl.dszczygiel.jdbc.nativeprotocol.message.responses.ResultKind;
import pl.dszczygiel.jdbc.nativeprotocol.message.responses.ResultMessage;
import pl.dszczygiel.jdbc.nativeprotocol.message.responses.Row;
import pl.dszczygiel.jdbc.nativeprotocol.message.responses.RowsResultFlags;
import pl.dszczygiel.jdbc.nativeprotocol.types.CQLTypeMetadata;

public class ResultMessageDecoder extends MessageDecoder {

	private final int RESULT_KIND_SIZE = 4;
	private final int FLAGS_SIZE = 4;
	private final int COLUMN_COUNT_SIZE = 4;
	private int currentPos = 0;

	ResultMessage result = new ResultMessage();
	FrameLayoutDecoders decoders = new FrameLayoutDecoders();
	List<ColumnSpecification> columnSpecifications;
	List<Row> rows;
	PreparedStatementData preparedStatementData;

	public ResultMessageDecoder(boolean containsWarning) {
		super(containsWarning);
	}

	@Override
	public Message decode(byte[] value) {
		if(isReadWarnings()) {
			int warnLen = decoders.decodeStringList(value, currentPos, result.getWarnings(), StandardCharsets.UTF_8);
			currentPos += warnLen;
		}
		
		int resultKindValue = decoders.decodeInt(value, currentPos, RESULT_KIND_SIZE);
		currentPos += RESULT_KIND_SIZE;

		ResultKind resultKind = ResultKind.getResultKind(resultKindValue);
		result.setKind(resultKind);

		if (resultKind == ResultKind.VOID)
			return result;
		if (resultKind == ResultKind.ROWS)
			decodeRowsTypeResult(value);
		if(resultKind == ResultKind.PREPARED)
			decodePreparedTypeResult(value);
		return result;
	}

	private void decodeRowsTypeResult(byte[] value) {
		rows = new ArrayList<Row>();
		columnSpecifications = new ArrayList<>();
		RowsResultFlags flags = new RowsResultFlags();
		PagingState pagingState;

		// flags
		int flagsBytes = decoders.decodeInt(value, currentPos, 4);
		if ((flagsBytes & 1) == 1)
			flags.setGlobalTablesSpecFlag(true);
		if ((flagsBytes & 2) == 2)
			flags.setHasMorePagesFlag(true);
		if ((flagsBytes & 4) == 4)
			flags.setNoMetadataFlag(true);
		currentPos += FLAGS_SIZE;
		result.setRowResultFlags(flags);
		// column count
		int columnCount = decoders.decodeInt(value, currentPos, COLUMN_COUNT_SIZE);
		currentPos += COLUMN_COUNT_SIZE;
		result.setColumnCount(columnCount);
		// paging state [optional]
		if (flags.isSetHasMorePagesFlag()) {
			int bytesLen = decoders.decodeInt(value, currentPos, currentPos + 4);
			currentPos += 4;
			byte[] pagingStateBytes = decoders.decodeBytes(value, currentPos, bytesLen);
			pagingState = new PagingState(pagingStateBytes);
			currentPos += pagingStateBytes.length;
			result.setPagingState(pagingState);
		}

		if (!flags.isSetNoMetadataFlag()) {
			result.setColumnSpecifications(
					decodeColumnSpecifications(value, columnCount, flags.isSetGlobalTablesSpecFlag()));
		}
 
		int nrOfRows = decoders.decodeInt(value, currentPos, 4);
		currentPos += 4; // TODO Type size in enum
		// rows
		for (int i = 0; i < nrOfRows; i++) {
			List<byte[]> values = new ArrayList<byte[]>();
			for (int j = 0; j < columnCount; j++) {
				int len = decoders.decodeInt(value, currentPos, 4);
				currentPos += 4;
				if (len < 0) {
					values.add(null);
					continue;
				}
				byte[] columnValue = Arrays.copyOfRange(value, currentPos, currentPos + len);
				for(byte b:columnValue) {
				//	System.out.print(String.format("%x ", b));
				}
				//System.out.println();
				currentPos += len;
				values.add(columnValue);
			}
			rows.add(new Row(values));
		}

		result.setRows(rows);
	}

	private void decodePreparedTypeResult(byte[] value) {
		// prepared data
		int preparedColumnCount;
		int preparedPrimaryKeysCount;
		List<Integer> pkIndexes = new ArrayList<Integer>();
		preparedStatementData = new PreparedStatementData();
		
		int len = decoders.decodeShort(value, currentPos);
		currentPos += 2;
		byte[] id = decoders.decodeBytes(value, currentPos, len);
		currentPos += len;
		RowsResultFlags preparedFlags = new RowsResultFlags();
		int flagsBytes = decoders.decodeInt(value, currentPos, 4);
		currentPos += 4;

		if ((flagsBytes & 1) == 1)
			preparedFlags.setGlobalTablesSpecFlag(true);

		preparedColumnCount = decoders.decodeInt(value, currentPos, 4);
		currentPos += 4;
		preparedPrimaryKeysCount = decoders.decodeInt(value, currentPos, 4);
		currentPos += 4;

		for (int i = 0; i < preparedPrimaryKeysCount; i++) {
			int key = decoders.decodeShort(value, currentPos);
			currentPos += 2;
			pkIndexes.add(key);
		}
		preparedStatementData.setColumnCount(preparedColumnCount);
		preparedStatementData.setId(id);
		preparedStatementData.setPrimaryKeyCount(preparedPrimaryKeysCount);
		preparedStatementData.setPrimaryKeysIndexes(pkIndexes);
		preparedStatementData.setColumnSpecifications(
				decodeColumnSpecifications(value, preparedColumnCount, preparedFlags.isSetGlobalTablesSpecFlag()));
		result.setPreparedStatementData(preparedStatementData);
		
		// prepared result data
		// flags
		RowsResultFlags prepResultFlags = new RowsResultFlags();
		int prepResultflagsBytes = decoders.decodeInt(value, currentPos, 4);
		currentPos+= 4;
		if ((prepResultflagsBytes & 1) == 1)
			prepResultFlags.setGlobalTablesSpecFlag(true);
		if ((flagsBytes & 2) == 2)
			prepResultFlags.setHasMorePagesFlag(true);
		if ((flagsBytes & 4) == 4)
			prepResultFlags.setNoMetadataFlag(true);
		
		int columnCount = decoders.decodeInt(value, currentPos, COLUMN_COUNT_SIZE);
		currentPos += COLUMN_COUNT_SIZE;
		
		if (!prepResultFlags.isSetNoMetadataFlag()) {
			result.setColumnSpecifications(
					decodeColumnSpecifications(value, columnCount, prepResultFlags.isSetGlobalTablesSpecFlag()));
		}
	}

	private List<ColumnSpecification> decodeColumnSpecifications(byte[] value, int columnCount, boolean globalSpec) {
		List<ColumnSpecification> mSpecs = new ArrayList<ColumnSpecification>();

		if (globalSpec) {
			String keyspaceName;
			String tableName;
			// keyspace name
			short strLen = decoders.decodeShort(value, currentPos);
			currentPos += 2;
			String s = decoders.decodeString(value, currentPos, strLen, StandardCharsets.UTF_8);
			keyspaceName = s;
			if (s != null)
				currentPos += s.length();

			// table name
			strLen = decoders.decodeShort(value, currentPos);
			currentPos += 2;
			s = decoders.decodeString(value, currentPos, strLen, StandardCharsets.UTF_8);
			tableName = s;
			if (s != null)
				currentPos += s.length();

			for (int i = 0; i < columnCount; i++) {
				ColumnSpecification cs = new ColumnSpecification();
				strLen = decoders.decodeShort(value, currentPos);
				currentPos += 2;
				s = decoders.decodeString(value, currentPos, strLen, StandardCharsets.UTF_8);
				if (s != null)
					currentPos += s.length();
				CQLTypeMetadata cqlTypeMetadata = decoders.decodeCqlType(value, currentPos);
				//short dataType = decoders.decodeShort(value, currentPos);
				currentPos += cqlTypeMetadata.getSize();

				cs.setTableName(tableName);
				cs.setKeyspaceName(keyspaceName);
				cs.setColumnName(s);
				cs.setColumnType(cqlTypeMetadata);
				mSpecs.add(cs);
			}
		} else {
			for (int i = 0; i < columnCount; i++) {
				ColumnSpecification cs = new ColumnSpecification();
				// keyspace name
				short strLen = decoders.decodeShort(value, currentPos);
				currentPos += 2;
				String s = decoders.decodeString(value, currentPos, strLen, StandardCharsets.UTF_8);
				if (s != null)
					currentPos += s.length();
				cs.setKeyspaceName(s);
				// table name
				strLen = decoders.decodeShort(value, currentPos);
				currentPos += 2;
				s = decoders.decodeString(value, currentPos, strLen, StandardCharsets.UTF_8);
				if (s != null)
					currentPos += s.length();
				cs.setTableName(s);
				// column name
				strLen = decoders.decodeShort(value, currentPos);
				currentPos += 2;
				s = decoders.decodeString(value, currentPos, strLen, StandardCharsets.UTF_8);
				if (s != null)
					currentPos += s.length();
				cs.setColumnName(s);
				CQLTypeMetadata cqlTypeMetadata = decoders.decodeCqlType(value, currentPos);

			//	short dataType = decoders.decodeShort(value, currentPos);
				currentPos += cqlTypeMetadata.getSize();
				cs.setColumnType(cqlTypeMetadata);
				mSpecs.add(cs);
			}
		}
		return mSpecs;
	}

}
