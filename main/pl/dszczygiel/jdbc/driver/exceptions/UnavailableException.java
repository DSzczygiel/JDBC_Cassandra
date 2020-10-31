package pl.dszczygiel.jdbc.driver.exceptions;

import pl.dszczygiel.jdbc.nativeprotocol.constants.Consistency;

public class UnavailableException extends CassandraException{
	private static final long serialVersionUID = -4677551272448988056L;
	private Consistency requiredConsistency;
	private int requiredNodes;
	private int aliveNodes;
	
	public UnavailableException(Consistency consistency, int required, int alive) {
		super(String.format("Consistency level %s requires %d nodes, but %d is alive.", consistency.name(), required, alive));
		this.requiredConsistency = consistency;
		this.aliveNodes = alive;
		this.requiredNodes = required;
	}

	public Consistency getRequiredConsistency() {
		return requiredConsistency;
	}

	public void setRequiredConsistency(Consistency requiredConsistency) {
		this.requiredConsistency = requiredConsistency;
	}

	public int getRequiredNodes() {
		return requiredNodes;
	}

	public void setRequiredNodes(int requiredNodes) {
		this.requiredNodes = requiredNodes;
	}

	public int getAliveNodes() {
		return aliveNodes;
	}

	public void setAliveNodes(int aliveNodes) {
		this.aliveNodes = aliveNodes;
	}

	
	
}
