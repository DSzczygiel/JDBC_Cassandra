package pl.dszczygiel.jdbc.nativeprotocol.message.responses;

import pl.dszczygiel.jdbc.nativeprotocol.constants.EventType;
import pl.dszczygiel.jdbc.nativeprotocol.frame.Message;

public class EventMessage extends Message{
	private EventType eventType;
	private TopologyChangeData topologyChangeData;
	private StatusChangeData statusChangeData;
	private SchemaChangeData schemaChangeData;
	
	public EventType getEventType() {
		return eventType;
	}
	public void setEventType(EventType eventType) {
		this.eventType = eventType;
	}
	public TopologyChangeData getTopologyChangeData() {
		return topologyChangeData;
	}
	public void setTopologyChangeData(TopologyChangeData topologyChangeData) {
		this.topologyChangeData = topologyChangeData;
	}
	public StatusChangeData getStatusChangeData() {
		return statusChangeData;
	}
	public void setStatusChangeData(StatusChangeData statusChangeData) {
		this.statusChangeData = statusChangeData;
	}
	public SchemaChangeData getSchemaChangeData() {
		return schemaChangeData;
	}
	public void setSchemaChangeData(SchemaChangeData schemaChangeData) {
		this.schemaChangeData = schemaChangeData;
	}
	
	
}
