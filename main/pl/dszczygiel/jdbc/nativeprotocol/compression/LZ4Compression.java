package pl.dszczygiel.jdbc.nativeprotocol.compression;


public class LZ4Compression implements Compression{

	@Override
	public byte[] compress(byte[] uncompressed) {
		throw new RuntimeException("LZ4 compression is not implemented");
	}

	@Override
	public byte[] decompress(byte[] compressed) {
		throw new RuntimeException("LZ4 compression is not implemented");
	}

}
