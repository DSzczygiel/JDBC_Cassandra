package pl.dszczygiel.jdbc.driver.exceptions;

public class UnauthorizedException extends CassandraException{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public UnauthorizedException(String message) {
		super(message);
	}

}
