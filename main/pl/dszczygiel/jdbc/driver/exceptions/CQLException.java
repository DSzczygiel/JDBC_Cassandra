package pl.dszczygiel.jdbc.driver.exceptions;

import java.sql.SQLException;

public class CQLException extends SQLException{

	/**
	 * 
	 */
	private static final long serialVersionUID = -406138479252867021L;
	
	public CQLException(String message) {
		super(message);
	}
}
