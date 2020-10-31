package pl.dszczygiel.jdbc.nativeprotocol.constants;

public enum Consistency {
	ANY,
	ONE,
	TWO,
	THREE,
	QUORUM,
	ALL,
	LOCAL_QUORUM,
	EACH_QUORUM,
	SERIAL,
	LOCAL_SERIAL,
	LOCAL_ONE;
	
	public static Consistency getByName(String name) {
		for(Consistency c : Consistency.values()) {
			if(c.name().equalsIgnoreCase(name)) {
				return c;
			}
		}
		return ONE;
	}
}
 	