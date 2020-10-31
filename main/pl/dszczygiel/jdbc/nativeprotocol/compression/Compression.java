package pl.dszczygiel.jdbc.nativeprotocol.compression;

public interface Compression {
	public byte[] compress(byte[] uncompressed);
	public byte[] decompress(byte[] compressed);	
}
