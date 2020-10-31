package pl.dszczygiel.jdbc.nativeprotocol.messages.requests;

import java.util.EnumSet;
import java.util.HashSet;
import java.util.Set;

import pl.dszczygiel.jdbc.nativeprotocol.constants.EventType;
import pl.dszczygiel.jdbc.nativeprotocol.frame.Message;

public class RegisterMessage extends Message{
	private Set<EventType> events = new HashSet<EventType>();
	
	public void removeEvent(EventType event) {
		events.remove(event);
	}
	
	public void addEvent(EventType event) {
		events.add(event);
	}
	
	public int eventsCount() {
		return events.size();
	}

	public Set<EventType> getEvents() {
		return events;
	}

	public void setEvents(EnumSet<EventType> events) {
		this.events = events;
	}
	
	
}
