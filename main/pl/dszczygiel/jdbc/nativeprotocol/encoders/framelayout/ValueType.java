package pl.dszczygiel.jdbc.nativeprotocol.encoders.framelayout;

import pl.dszczygiel.jdbc.nativeprotocol.constants.CQLType;
import pl.dszczygiel.jdbc.nativeprotocol.types.CQLTypeMetadata;

public class ValueType {
	private CQLTypeMetadata type;
	private Object value;
	
	public ValueType(CQLTypeMetadata type, Object value) {
		super();
		this.type = type;
		this.value = value;
	}
	
	public CQLTypeMetadata getType() {
		return type;
	}
	public Object getValue() {
		return value;
	}
	
	
}
