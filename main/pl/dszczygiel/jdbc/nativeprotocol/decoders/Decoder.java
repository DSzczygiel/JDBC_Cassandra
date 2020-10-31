package pl.dszczygiel.jdbc.nativeprotocol.decoders;

public interface Decoder<T> {
	T decode(byte[] value);
}
