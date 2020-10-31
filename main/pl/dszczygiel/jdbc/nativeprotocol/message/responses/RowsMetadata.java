package pl.dszczygiel.jdbc.nativeprotocol.message.responses;

import pl.dszczygiel.jdbc.driver.PagingState;

public class RowsMetadata {
	private RowsResultFlags flags;
	private int columnCount;
	private PagingState pagingState;
	private String globalSpecKeyspace;
	private String globalSpecTable;
}
