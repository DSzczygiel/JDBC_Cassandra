package pl.dszczygiel.jdbc.driver;

import pl.dszczygiel.jdbc.nativeprotocol.message.responses.TopologyChangeData;

public interface TopologyChangeListener {
	void onTopologyChange(TopologyChangeData topologyChangeData);
}
