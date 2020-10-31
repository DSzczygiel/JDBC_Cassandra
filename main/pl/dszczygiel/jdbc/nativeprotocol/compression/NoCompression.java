package pl.dszczygiel.jdbc.nativeprotocol.compression;

public class NoCompression implements Compression{

	@Override
	public byte[] compress(byte[] uncompressed) {
		return uncompressed;
	}

	@Override
	public byte[] decompress(byte[] compressed) {
		return compressed;
	}

}
