package pl.dszczygiel.jdbc.nativeprotocol.frame;

public class HeaderFlags {
	private boolean compressionFlag;
	private boolean tracingFlag;
	private boolean customPayloadFlag;
	private boolean warningFlag;
	
	public HeaderFlags() {
		this.compressionFlag = false;
		this.tracingFlag = false;
		this.customPayloadFlag = false;
		this.warningFlag = false;
	}
	
	public boolean getCompressionFlag() {
		return compressionFlag;
	}
	public void setCompressionFlag(boolean compressionFlag) {
		this.compressionFlag = compressionFlag;
	}
	public boolean getTracingFlag() {
		return tracingFlag;
	}
	public void setTracingFlag(boolean tracingFlag) {
		this.tracingFlag = tracingFlag;
	}
	public boolean getCustomPayloadFlag() {
		return customPayloadFlag;
	}
	public void setCustomPayloadFlag(boolean customPayloadFlag) {
		this.customPayloadFlag = customPayloadFlag;
	}
	public boolean getWarningFlag() {
		return warningFlag;
	}
	public void setWarningFlag(boolean warningFlag) {
		this.warningFlag = warningFlag;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (compressionFlag ? 1231 : 1237);
		result = prime * result + (customPayloadFlag ? 1231 : 1237);
		result = prime * result + (tracingFlag ? 1231 : 1237);
		result = prime * result + (warningFlag ? 1231 : 1237);
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		HeaderFlags other = (HeaderFlags) obj;
		if (compressionFlag != other.compressionFlag)
			return false;
		if (customPayloadFlag != other.customPayloadFlag)
			return false;
		if (tracingFlag != other.tracingFlag)
			return false;
		if (warningFlag != other.warningFlag)
			return false;
		return true;
	}
	
	
}
