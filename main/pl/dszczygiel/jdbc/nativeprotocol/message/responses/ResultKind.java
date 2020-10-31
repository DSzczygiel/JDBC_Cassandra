package pl.dszczygiel.jdbc.nativeprotocol.message.responses;

public enum ResultKind {
	VOID(1),
	ROWS(2),
	SET_KEYSPACE(3),
	PREPARED(4),
	SCHEMA_CHANGE(5);
	
	public final int value;
	private ResultKind(int value) {
		this.value = value;
	}
	
	public static ResultKind getResultKind(int value) {
		for(ResultKind kind : ResultKind.values()) {
			if(kind.value == value)
				return kind;
		}
		return VOID;
	}
}
