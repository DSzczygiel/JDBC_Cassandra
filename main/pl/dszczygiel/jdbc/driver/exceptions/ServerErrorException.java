package pl.dszczygiel.jdbc.driver.exceptions;

public class ServerErrorException extends CassandraException{

	/**
	 * 
	 */
	private static final long serialVersionUID = 7621657528447703965L;

	public ServerErrorException(String message) {
		super(message);
	}

}
