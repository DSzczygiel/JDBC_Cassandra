package pl.dszczygiel.jdbc.nativeprotocol.message.responses;

import java.util.List;

import pl.dszczygiel.jdbc.driver.exceptions.CQLException;
import pl.dszczygiel.jdbc.nativeprotocol.decoders.CQLDeserializer;
import pl.dszczygiel.jdbc.nativeprotocol.types.CQLTypeMetadata;

public class Row {
	private List<byte[]> rowValues;

	public Row(List<byte[]> values){
		this.rowValues = values;
	}
	
	public CQLTypeMetadata getTypeByIndex(int index, List<ColumnSpecification> specs) throws CQLException {
		if(index >= rowValues.size())
			throw new CQLException("Column index out of bounds");
		
		return specs.get(index).getColumnType();
	}
	
	public CQLTypeMetadata getTypeByName(String name, List<ColumnSpecification> specs) throws CQLException {
		for(ColumnSpecification cs : specs) {
			if(cs.getColumnName().equals(name)) {
				int position = specs.indexOf(cs);
				return cs.getColumnType();
			}
		}
		throw new CQLException("Column name not found");
	}
	
	public Object getValueByIndex(int index, List<ColumnSpecification> specs) throws CQLException {
		Object value;
		CQLDeserializer deserializer = new CQLDeserializer();

		if(index >= rowValues.size())
			throw new CQLException("Column index out of bounds");
		
		value = deserializer.deserialize(specs.get(index).getColumnType(), rowValues.get(index));
		return value;
	}
	
	public Object getValueByName(String name, List<ColumnSpecification> specs) throws CQLException {
		Object value;
		CQLDeserializer deserializer = new CQLDeserializer();

		for(ColumnSpecification cs : specs) {
			if(cs.getColumnName().equals(name)) {
				int position = specs.indexOf(cs);
				value = deserializer.deserialize(cs.getColumnType(), rowValues.get(position));
				return value;
			}
		}
		throw new CQLException("Column name not found");
	}
}
