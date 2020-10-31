package pl.dszczygiel.jdbc.utils;

import pl.dszczygiel.jdbc.nativeprotocol.constants.CQLType;
import pl.dszczygiel.jdbc.nativeprotocol.types.CQLTypeMetadata;

public class CQLUtils {
	public static CQLTypeMetadata getTypeMetadataFromText(String text) {
		if(text.equals("text"))
			text = "varchar";
		
		CQLTypeMetadata meta = null ;
		if(text.startsWith("list")) {
			
		}else if(text.startsWith("set")) {
			
		}else if(text.startsWith("map")) {
			
		}else {
			return new CQLTypeMetadata(CQLType.getTypeByName(text));
		}
		return meta;
	}
}
