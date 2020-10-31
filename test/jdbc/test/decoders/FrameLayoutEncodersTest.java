package jdbc.test.decoders;

import static org.junit.jupiter.api.Assertions.*;

import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;

import pl.dszczygiel.jdbc.nativeprotocol.constants.Consistency;
import pl.dszczygiel.jdbc.nativeprotocol.encoders.framelayout.FrameLayoutEncoders;

class FrameLayoutEncodersTest {
	FrameLayoutEncoders encoders = new FrameLayoutEncoders();
	
	@Test
	void encodeIntTest() {
		Integer i0 = 0;
		Integer imin = Integer.MIN_VALUE;
		Integer imax = Integer.MAX_VALUE;
		Integer ipos = 5555;
		Integer ineg = -5555;
		Integer iminone = -1;
		
		byte[] b0 = {0x00, 0x00, 0x00, 0x00};
		byte[] bmin = {(byte) 0x80, 0x00, 0x00, 0x00};
		byte[] bmax = {0x7F, (byte)0xFF , (byte)0xFF, (byte)0xFF};
		byte[] bpos = {0x00, 0x00, 0x15 ,(byte) 0xB3};
		byte[] bneg = {(byte)0xFF, (byte)0xFF, (byte)0xEA ,0x4D};
		byte[] bminone = {(byte)0xFF,(byte)0xFF,(byte)0xFF,(byte)0xFF};

		assertArrayEquals(encoders.encodeInt(i0), b0);
		assertArrayEquals(encoders.encodeInt(imin), bmin);
		assertArrayEquals(encoders.encodeInt(imax), bmax);
		assertArrayEquals(encoders.encodeInt(ipos), bpos);
		assertArrayEquals(encoders.encodeInt(ineg), bneg);
		assertArrayEquals(encoders.encodeInt(iminone), bminone);

	}
	
	@Test
	void encodeShortTest() {
		Short s0 = 0;
		Short smin = Short.MIN_VALUE;
		Short smax = Short.MAX_VALUE;
		Short spos = 5555;
		Short sneg = -5555;
		
		byte[] b0 = {0x00, 0x00};
		byte[] bmin = {(byte) 0x80, 0x00};
		byte[] bmax = {0x7F, (byte)0xFF};
		byte[] bpos = {0x15 ,(byte) 0xB3};
		byte[] bneg = {(byte)0xEA ,0x4D};
		
		assertArrayEquals(b0, encoders.encodeShort(s0));
		assertArrayEquals(bmin, encoders.encodeShort(smin));
		assertArrayEquals(bmax, encoders.encodeShort(smax));
		assertArrayEquals(bpos, encoders.encodeShort(spos));
		assertArrayEquals(bneg, encoders.encodeShort(sneg));		
	}
	
	@Test
	void encodeLongTest() {
		Long l0 = 0L;
		Long lmin = Long.MIN_VALUE;
		Long lmax = Long.MAX_VALUE;
		Long lpos = 5555L;
		Long lneg = -5555L;
		
		byte[] b0 = {0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00};
		byte[] bmin = {(byte) 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00};
		byte[] bmax = {0x7F, (byte)0xFF , (byte)0xFF, (byte)0xFF, (byte)0xFF, (byte)0xFF, (byte)0xFF, (byte)0xFF};
		byte[] bpos = {0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x15 ,(byte) 0xB3};
		byte[] bneg = {(byte)0xFF, (byte)0xFF, (byte)0xFF, (byte)0xFF, (byte)0xFF, (byte)0xFF, (byte)0xEA ,0x4D};

		assertArrayEquals(encoders.encodeLong(l0), b0);
		assertArrayEquals(encoders.encodeLong(lmin), bmin);
		assertArrayEquals(encoders.encodeLong(lmax), bmax);
		assertArrayEquals(encoders.encodeLong(lpos), bpos);
		assertArrayEquals(encoders.encodeLong(lneg), bneg);
	}
	
	@Test
	void encodeLongStringTest() {
		String s = "12345";
		String sempty = "";
		String snull = null;
		
		byte[] b = {0x00, 0x00, 0x00, 0x05, 0x31, 0x32, 0x33, 0x34, 0x35};
		byte[] bempty = {0x00, 0x00, 0x00, 0x00};
		byte[] bnull = {(byte)0xFF, (byte)0xFF, (byte)0xFF, (byte)0xFF};
		
		assertArrayEquals(b, encoders.encodeLongString(s));
		assertArrayEquals(bempty, encoders.encodeLongString(sempty));
		assertArrayEquals(bnull, encoders.encodeLongString(snull));
	}
	
	@Test
	void encodeStringTest() {
		String s = "12345";
		String sempty = "";
		String snull = null;
		
		byte[] b = {0x00, 0x05, 0x31, 0x32, 0x33, 0x34, 0x35};
		byte[] bempty = {0x00, 0x00};
		byte[] bnull = {(byte)0xFF, (byte)0xFF};
		
		assertArrayEquals(b, encoders.encodeString(s));
		assertArrayEquals(bempty, encoders.encodeString(sempty));
		assertArrayEquals(bnull, encoders.encodeString(snull));
	}
	
	@Test
	void encodeConsistencyTest() {
		Consistency c = Consistency.ALL;
		Consistency cnull = null;
		
		byte[] b = {0x00, 0x05};
		byte[] bnull = {0x00, 0x01};
		
		assertArrayEquals(b, encoders.encodeConsistency(c));
		assertArrayEquals(bnull, encoders.encodeConsistency(cnull));
	}

	@Test
	void encodeBytesTest() {
		byte[] bytes = {0x01, 0x02, 0x03, 0x04};
		byte[] bytesempty = new byte[0];
		byte[] bytesnull = null;
		
		byte[] b = {0x00, 0x00, 0x00, 0x04, 0x01, 0x02, 0x03, 0x04};
		byte[] bempty = {0x00, 0x00, 0x00, 0x00};
		byte[] bnull = {(byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF};
		
		assertArrayEquals(b, encoders.encodeBytes(bytes));
		assertArrayEquals(bempty, encoders.encodeBytes(bytesempty));
		assertArrayEquals(bnull, encoders.encodeBytes(bytesnull));
	}
	
	@Test
	void encodeStringMapTest() {
		Map<String, String> m = new HashMap<String, String>();
		m.put("a", "bc");
		m.put("ab", "cde");
		Map<String, String> mempty = new HashMap<String, String>();
		Map<String, String> mnull = null;
		byte[] b = {0x00, 0x02, //mapLen 
				0x00, 0x01, //k1Len
				0x61, //k1
				0x00, 0x02, //v1Len
				0x62, 0x63, //v1
				0x00, 0x02, //k2Len
				0x61, 0x62, //k2
				0x00, 0x03, //v2Len
				0x63, 0x64, 0x65 // v2
				};
		byte[] bempty = {0x00, 0x00};
		byte[] bnull = {(byte) 0xFF, (byte) 0xFF};
		
		assertArrayEquals(b, encoders.encodeStringMap(m));
		assertArrayEquals(bempty, encoders.encodeStringMap(mempty));
		assertArrayEquals(bnull, encoders.encodeStringMap(mnull));

	}
	
	@Test
	void encodeValueTest() {
		String string = "abcd";
		Integer integer = 16;
		
		//ValueType v1 = new ValueType(type, value)
	}
}
