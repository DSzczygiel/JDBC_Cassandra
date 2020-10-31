package pl.dszczygiel.jdbc.driver.exceptions;

public class InvalidQueryException extends CassandraException{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public InvalidQueryException(String message) {
		super(message);
	}

}
