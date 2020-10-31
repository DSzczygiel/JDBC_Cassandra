package jdbc.test.sqlcql;

import org.junit.jupiter.api.Test;

import net.sf.jsqlparser.JSQLParserException;
import pl.dszczygiel.jdbc.driver.sqlcql.JoinStatement;

public class JoinTest {

	@Test
	public void joinTest() throws JSQLParserException {
		String joinString = "SELECT user.user_id, user.first_name, user.last_name, "
				+ "user_login.login_id, user_login.login, user_login.password, user_login.user_id FROM "
				+ "user_login " 
				+ "INNER JOIN user ON user.user_id = user_login.user_id";
		
		JoinStatement js = new JoinStatement(joinString);
		
		System.out.println(js);
	}
}
