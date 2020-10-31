package pl.dszczygiel.jdbc.driver;

public interface Authenticator {
	byte[] onAuthInit();
	byte[] onAuthChallenge(byte[] data);
	void onAuthSuccess(byte[] data);
}
