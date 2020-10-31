package pl.dszczygiel.jdbc.driver.exceptions;

public enum ErrorCode {
	SERVER_ERROR(0), PROTOCOL_ERROR(0x000A), AUTHENTICATION_ERROR(0x0100), UNAVAILABLE_EXCEPTION(0x1000),
	OVERLOADED(0x1001), IS_BOOTSTRAPING(0x1002), TRUNCATE_ERROR(0x1003), WRITE_TIMEOUT(0x1100), READ_TIMEOUT(0x1200),
	READ_FAILURE(0x1300), FUNCTION_FAILURE(0x1400), WRITE_FAILURE(0x1500), SYNTAX_ERROR(0x2000),
	UNAUTHORIZED(0x2100), INVALID_QUERY(0x2200), CONFIG_ERROR(0x2300), ALREADY_EXISTS(0x2400),
	UNPREPARED(0x2500);

	private int code;

	private ErrorCode(int code) {
		this.code = code;
	}
	
	public static ErrorCode getByCode(int code) {
		for(ErrorCode ec : ErrorCode.values()) {
			if(ec.code == code)
				return ec;
		}
		return null;
	}
}
