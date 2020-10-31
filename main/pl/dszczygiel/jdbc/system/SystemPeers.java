package pl.dszczygiel.jdbc.system;

import java.net.InetAddress;
import java.util.List;

import pl.dszczygiel.jdbc.driver.exceptions.CQLException;
import pl.dszczygiel.jdbc.nativeprotocol.message.responses.ResultMessage;
import pl.dszczygiel.jdbc.nativeprotocol.message.responses.Row;

public class SystemPeers {
	List<Peer> peers;

	public SystemPeers(ResultMessage message) {
		List<Row> rows = message.getRows();
		for(Row r : rows) {
			Peer peer = new Peer();
			InetAddress addr = null;
			InetAddress prefAddr = null;
			try {
				peer.setDatacenter((String) r.getValueByName("data_center", message.getColumnSpecifications()));
				peer.setRack((String) r.getValueByName("rack", message.getColumnSpecifications()));
				addr = (InetAddress) r.getValueByName("peer", message.getColumnSpecifications());
				prefAddr = (InetAddress) r.getValueByName("preferred_ip", message.getColumnSpecifications());
				peer.setPeerAddress(addr);
				peer.setPreferedAddress(prefAddr);
			} catch (CQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}
	}

	public List<Peer> getPeers() {
		return peers;
	}

	public void setPeers(List<Peer> peers) {
		this.peers = peers;
	}
	
	
}
