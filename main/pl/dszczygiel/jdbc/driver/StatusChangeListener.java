package pl.dszczygiel.jdbc.driver;

import pl.dszczygiel.jdbc.nativeprotocol.message.responses.StatusChangeData;

public interface StatusChangeListener {
	void onStatusChange(StatusChangeData statusChangeData);
}
