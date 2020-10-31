package pl.dszczygiel.jdbc.driver;

import pl.dszczygiel.jdbc.nativeprotocol.message.responses.SchemaChangeData;

public interface SchemaChangeListener {
	void onSchemaChange(SchemaChangeData schemaChangeData);
}
