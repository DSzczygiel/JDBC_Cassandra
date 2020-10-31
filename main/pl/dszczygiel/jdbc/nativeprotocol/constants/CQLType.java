package pl.dszczygiel.jdbc.nativeprotocol.constants;

public enum CQLType {
	CUSTOM(0), ASCII(1), BIGINT(2), BLOB(3), BOOLEAN(4), COUNTER(5), DECIMAL(6), DOUBLE(7), FLOAT(8), INT(9),
	TIMESTAMP(11), UUID(12), VARCHAR(13), VARINT(14), TIMEUUID(15), INET(16), DATE(17), TIME(18), SMALLINT(19), TINYINT(20),
	LIST(32), MAP(33), SET(34), UDT(48), TUPLE(49);

	public final int value;

	CQLType(int value) {
		this.value = value;
	}
	
	public static CQLType getType(int value) {
		for(CQLType type : CQLType.values()) {
			if(type.value == value)
				return type;
				
		}
		return CUSTOM;
	}
	
	public static CQLType getTypeByName(String name) {
		for(CQLType type : CQLType.values()) {
			if(type.name().equalsIgnoreCase(name))
				return type;
				
		}
		return CUSTOM;
	}
}
