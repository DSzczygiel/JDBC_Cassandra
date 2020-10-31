package pl.dszczygiel.jdbc.nativeprotocol.constants;

public enum CqlVersion {
	v3_0_0("3.0.0");
	
	private String version;
	
	private CqlVersion(String version) {
		this.version = version;
	}
	
	public String getVersion() {
		return this.version;
	}
}
