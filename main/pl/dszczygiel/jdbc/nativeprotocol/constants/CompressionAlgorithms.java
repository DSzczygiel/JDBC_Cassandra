package pl.dszczygiel.jdbc.nativeprotocol.constants;

public enum CompressionAlgorithms {
	LZ4, 
	SNAPPY,
	NO_COMPRESSION;
	
	public static CompressionAlgorithms getByName(String name) {
		for(CompressionAlgorithms ca : CompressionAlgorithms.values()) {
			if(ca.name().equalsIgnoreCase(name)) {
				return ca;
			}
		}
		return NO_COMPRESSION;
	}
}
