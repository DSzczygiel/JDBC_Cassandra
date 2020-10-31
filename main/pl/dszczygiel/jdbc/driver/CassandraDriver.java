package pl.dszczygiel.jdbc.driver;

import java.io.IOException;
import java.net.UnknownHostException;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;

import org.slf4j.LoggerFactory;

public class CassandraDriver implements Driver{
	private static final org.slf4j.Logger logger = LoggerFactory.getLogger(CassandraDriver.class);
	
    static { 
        try {
            java.sql.DriverManager.registerDriver(new CassandraDriver());
        } catch (SQLException E) {
            throw new RuntimeException("Can't register driver!");
        }
    }
    
    public CassandraDriver() {
	}
    
	@Override
	public boolean acceptsURL(String url) throws SQLException {
		if(url.startsWith("jdbc:dszczygiel:cassandra"))
			return true;
		return false;
	}

	@Override
	public Connection connect(String arg0, Properties arg1) throws SQLException {
		// TODO Auto-generated method stub
		try {
			if(!acceptsURL(arg0))	
				throw new SQLException("Connection string should start fron jdbc:dszczygiel:cassandra");
			return new CassandraConnection(arg0, arg1);
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}
	

	@Override
	public int getMajorVersion() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getMinorVersion() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public Logger getParentLogger() throws SQLFeatureNotSupportedException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public DriverPropertyInfo[] getPropertyInfo(String arg0, Properties arg1) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean jdbcCompliant() {
		// TODO Auto-generated method stub
		return false;
	}

}
