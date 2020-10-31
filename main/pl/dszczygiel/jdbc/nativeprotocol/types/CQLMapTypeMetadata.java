package pl.dszczygiel.jdbc.nativeprotocol.types;

import pl.dszczygiel.jdbc.nativeprotocol.constants.CQLType;

public class CQLMapTypeMetadata extends CQLTypeMetadata{

	private CQLType keyType;
	private CQLType valueType;
	
	public CQLMapTypeMetadata(CQLType type, CQLType keyType, CQLType valueType) {
		super(type);
		this.keyType = keyType;
		this.valueType = valueType;
	}

	public CQLType getKeyType() {
		return keyType;
	}

	public CQLType getValueType() {
		return valueType;
	}	
	
	@Override
	public int getSize() {
		return 6;
	}

}
