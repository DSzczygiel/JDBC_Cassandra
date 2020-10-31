package pl.dszczygiel.jdbc.nativeprotocol.types;

import pl.dszczygiel.jdbc.nativeprotocol.constants.CQLType;

public class CQLTypeMetadata {
	private CQLType type;
	public CQLType getType() {
		return this.type;
	}
	public CQLTypeMetadata(CQLType type) {
		this.type = type;
	}
	public int getSize() {
		return 2;
	}
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((type == null) ? 0 : type.hashCode());
		return result;
	}
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		CQLTypeMetadata other = (CQLTypeMetadata) obj;
		if (type != other.type)
			return false;
		return true;
	}
	
	
}
