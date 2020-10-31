package pl.dszczygiel.jdbc.system;

import java.util.TreeMap;

public class CassandraPrimaryKey {
	String partitionKey;
	TreeMap<Integer, String> clusteringKeys = new TreeMap<>();

	public String getPartitionKey() {
		return partitionKey;
	}

	public void setPartitionKey(String partitionKey) {
		this.partitionKey = partitionKey;
	}

	public TreeMap<Integer, String> getClusteringKeys() {
		return clusteringKeys;
	}

	public void setClusteringKeys(TreeMap<Integer, String> clusteringKeys) {
		this.clusteringKeys = clusteringKeys;
	}

}
