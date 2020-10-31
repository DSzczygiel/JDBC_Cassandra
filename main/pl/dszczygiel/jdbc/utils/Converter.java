package pl.dszczygiel.jdbc.utils;

import java.util.List;

public class Converter {

	public static int byteArrayToInt(byte[] arr, int from, int to) {
		int tmp = 0;
		for(int i=0; i<to-from; i++) {
			tmp |= (arr[from+i] & 0xFF) << i*8; 
		}
		return tmp;
	}
	
	public static String byteArrayToString(byte[] arr, int from, int to) {
		StringBuilder sb = new StringBuilder();
		int strLen = to-from;
		for (int i = 0; i < strLen; i++) {
			sb.append((char) arr[from + i]);
		}
		return sb.toString();
	}
	
	public static byte[] byteListToByteArray(List<Byte> blist) {
		if(blist == null) {
			return new byte[0];
		}
		
		byte[] barray = new byte[blist.size()];
		
		for(int i=0; i<blist.size(); i++) {
			barray[i] = blist.get(i);
		}
		
		return barray;
	}
	
	public static byte[] hexStringToByteArray(String s) {
	    int len = s.length();
	    byte[] data = new byte[len / 2];
	    for (int i = 0; i < len; i += 2) {
	        data[i / 2] = (byte) ((Character.digit(s.charAt(i), 16) << 4)
	                             + Character.digit(s.charAt(i+1), 16));
	    }
	    return data;
	}
}
