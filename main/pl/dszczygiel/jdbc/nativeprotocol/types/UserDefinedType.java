package pl.dszczygiel.jdbc.nativeprotocol.types;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import pl.dszczygiel.jdbc.driver.exceptions.CQLException;
import pl.dszczygiel.jdbc.nativeprotocol.encoders.framelayout.ValueType;

public class UserDefinedType {
	
	private CQLUdtMetadata meta;
	private Map<String, Object> fields;
	
	public UserDefinedType(CQLUdtMetadata metadata) {
		this.meta = metadata;
		fields = new LinkedHashMap<String, Object>();
	}
	
	public void addField(String name, ValueType value) {
		fields.put(name, value);
	}

	public String getKeyspace() {
		return meta.getKeyspace();
	}

	public String getName() {
		return meta.getName();
	}

	public String getString(String name) throws SQLException {
		validFieldName(name);
		return (String) fields.get(name);
	}
	
	public int getInt(String name) throws SQLException {
		validFieldName(name);
		return (int) fields.get(name);
	}
	
	public void setString(String name, String value) throws SQLException {
		validFieldName(name);
		fields.put(name, value);
	}
	
	public void setInt(String name, int value) throws SQLException {
		validFieldName(name);
		fields.put(name, value);
	}
	
	private void validFieldName(String name) throws CQLException {
		if(!meta.getFieldNames().contains(name))
			throw new CQLException("UDT " + getName() + " doesn't contain field " + name);
	}

	public Map<String, Object> getFields() {
		return fields;
	}

	public CQLUdtMetadata getMeta() {
		return meta;
	}
	
}
