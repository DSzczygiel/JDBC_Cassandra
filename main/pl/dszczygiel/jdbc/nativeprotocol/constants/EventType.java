package pl.dszczygiel.jdbc.nativeprotocol.constants;

public enum EventType {
	TOPOLOGY_CHANGE("TOPOLOGY_CHANGE"), STATUS_CHANGE("STATUS_CHANGE"), SCHEMA_CHANGE("SCHEMA_CHANGE");
	
	private String name;
	
	private EventType(String name) {
		this.name = name;
	}
	
	public static EventType getByName(String name) {
		for(EventType e : EventType.values()) {
			if(e.name.equals(name))
				return e;
		}
		return null;
	}
}
