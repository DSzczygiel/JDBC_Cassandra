package pl.dszczygiel.jdbc.driver.sqlcql;

import org.slf4j.LoggerFactory;

public class CreateDatabaseStatement {
	private static final org.slf4j.Logger logger = LoggerFactory.getLogger(CreateDatabaseStatement.class);

	String statement = null;
	private final String DEFAULT_REPLICATION_OPTIONS = " WITH REPLICATION = {'class':'SimpleStrategy', 'replication_factor' : 1}";

	public CreateDatabaseStatement(String statement) {
		String s = statement.replaceAll("(?i)database", "KEYSPACE");
		s += DEFAULT_REPLICATION_OPTIONS;
		this.statement = s;
		logger.warn("Using default replication: REPLICATION = {'class':'SimpleStrategy', 'replication_factor' : 1}");
	}

	@Override
	public String toString() {
		return statement;
	}

}