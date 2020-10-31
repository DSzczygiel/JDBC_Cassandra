package pl.dszczygiel.jdbc.driver;

public class PagingState {
	private byte[] value;

	public PagingState(byte[] value) {
		this.value = value;
	}
	
	public boolean isEmpty() {
		return this.value == null;
	}
	
	public byte[] getValue() {
		return this.value;
	}
}
