package pl.dszczygiel.jdbc.nativeprotocol.constants;

public enum OpCode {
	ERROR(0), STARTUP(1), READY(2), AUTHENTICATE(3), OPTIONS(5), SUPPORTED(6), QUERY(7), RESULT(8), PREPARE(9),
	EXECUTE(10), REGISTER(11), EVENT(12), BATCH(13), AUTH_CHALLENGE(14), AUTH_RESPONSE(15), AUTH_SUCCESS(16);

	public final int value;

	OpCode(final int value) {
		this.value = value;
	}
	
	
	public static OpCode getOpCode(int value) {
		for(OpCode code : OpCode.values()) {
			if(code.value == value)
				return code;
		}
		return ERROR;
	}
}
