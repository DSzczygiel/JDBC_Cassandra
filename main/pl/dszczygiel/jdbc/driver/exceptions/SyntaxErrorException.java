package pl.dszczygiel.jdbc.driver.exceptions;

public class SyntaxErrorException extends CassandraException{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public SyntaxErrorException(String message) {
		super(message);
	}

}
