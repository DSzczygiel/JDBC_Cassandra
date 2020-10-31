package pl.dszczygiel.jdbc.driver;

import java.sql.SQLWarning;

public class CassandraWarning extends SQLWarning{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@Override
	public SQLWarning getNextWarning() {
		// TODO Auto-generated method stub
		return super.getNextWarning();
	}

	@Override
	public void setNextWarning(SQLWarning w) {
		// TODO Auto-generated method stub
		super.setNextWarning(w);
	}
	
	

}
