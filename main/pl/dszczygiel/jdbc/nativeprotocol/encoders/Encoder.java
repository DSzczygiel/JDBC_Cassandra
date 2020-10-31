package pl.dszczygiel.jdbc.nativeprotocol.encoders;

public interface Encoder<T> {
	byte[] encode(T value);
}
