package pl.dszczygiel.jdbc.driver.exceptions;

public class ProtocolErrorException extends CassandraException{

	/**
	 * 
	 */
	private static final long serialVersionUID = 183977802089007463L;

	public ProtocolErrorException(String message) {
		super(message);
	}

}
