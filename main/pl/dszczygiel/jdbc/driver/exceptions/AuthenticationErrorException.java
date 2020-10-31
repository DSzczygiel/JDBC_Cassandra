package pl.dszczygiel.jdbc.driver.exceptions;

public class AuthenticationErrorException extends CassandraException{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public AuthenticationErrorException(String message) {
		super(message);
	}

}
