package pl.dszczygiel.jdbc.driver.exceptions;

public class UnpreparedException extends CassandraException{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public UnpreparedException(String message) {
		super(message);
	}

}
