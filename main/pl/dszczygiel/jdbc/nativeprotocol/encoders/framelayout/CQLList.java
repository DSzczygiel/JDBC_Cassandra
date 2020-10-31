package pl.dszczygiel.jdbc.nativeprotocol.encoders.framelayout;

import java.util.List;

import pl.dszczygiel.jdbc.nativeprotocol.constants.CQLType;

public class CQLList<T> {
	private List<T> list;
	private CQLType valuesType;
	public List<T> getList() {
		return list;
	}
	public void setList(List<T> list) {
		this.list = list;
	}
	public CQLType getValuesType() {
		return valuesType;
	}
	public void setValuesType(CQLType valuesType) {
		this.valuesType = valuesType;
	}
	
	
}
