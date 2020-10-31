package pl.dszczygiel.jdbc.nativeprotocol.types;

import java.util.List;
import java.util.Set;

import pl.dszczygiel.jdbc.nativeprotocol.constants.CQLType;

public class CQLUdtMetadata extends CQLTypeMetadata {
	private String keyspace;
	private String name;
	private List<String> fieldNames;
	private List<CQLTypeMetadata> fieldTypes;

	public CQLUdtMetadata(CQLType type, String keyspace, String name, List<String> fieldNames,
			List<CQLTypeMetadata> fieldTypes) {
		super(type);
		this.keyspace = keyspace;
		this.name = name;
		this.fieldNames = fieldNames;
		this.fieldTypes = fieldTypes;
	}

	@Override
	public int getSize() {
		int size = 2;
		size += 2; // string keyspace
		size += keyspace.length();
		size += 2; // string name;
		size += name.length();
		size += 2; // nrOfFields

		for (String s : fieldNames) {
			size += 2; // string fieldName
			size += s.length();
			size += 2; // fieldType

		}
		return size;
	}

	public String getKeyspace() {
		return keyspace;
	}

	public String getName() {
		return name;
	}

	public List<String> getFieldNames() {
		return fieldNames;
	}

	public List<CQLTypeMetadata> getFieldTypes() {
		return fieldTypes;
	}
	
	

}
