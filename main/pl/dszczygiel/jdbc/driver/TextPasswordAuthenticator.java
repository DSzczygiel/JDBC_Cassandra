package pl.dszczygiel.jdbc.driver;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;

public class TextPasswordAuthenticator implements Authenticator{
	String username;
	String password;
	
	public TextPasswordAuthenticator(String username, String password) throws SQLException {
		if(username == null || password == null)
			throw new SQLException("Authentication by password required. Please set username and password");
		this.username = username;
		this.password = password;
	}

	@Override
	public byte[] onAuthInit() {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		byte[] usernameBytes = username.getBytes(StandardCharsets.UTF_8);
		byte[] passwordBytes = password.getBytes(StandardCharsets.UTF_8);
		baos.write(0);
		baos.write(usernameBytes, 0, usernameBytes.length);
		baos.write(0);
		baos.write(passwordBytes, 0, passwordBytes.length);
		
		return baos.toByteArray();
	}

	@Override
	public byte[] onAuthChallenge(byte[] data) {
		return null;
	}

	@Override
	public void onAuthSuccess(byte[] data) {
		
	}

}
