package pl.dszczygiel.jdbc.nativeprotocol.types;

import pl.dszczygiel.jdbc.nativeprotocol.constants.CQLType;

public class CQLListSetTypeMetadata extends CQLTypeMetadata{

	private CQLType elementsType;
	
	public CQLListSetTypeMetadata(CQLType type, CQLType elemntsType) {
		super(type);
		this.elementsType = elemntsType;
	}

	public CQLType getElementsType() {
		return elementsType;
	}
	
	@Override
	public int getSize() {
		return 4;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + ((elementsType == null) ? 0 : elementsType.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (!super.equals(obj))
			return false;
		if (getClass() != obj.getClass())
			return false;
		CQLListSetTypeMetadata other = (CQLListSetTypeMetadata) obj;
		if (elementsType != other.elementsType)
			return false;
		return true;
	}
	
	
	
}
