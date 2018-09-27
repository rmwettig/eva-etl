package de.ingef.eva.utility;

import static org.junit.Assert.*;

import org.junit.Test;

public class HelperTest {

	@Test
	public void padToLengthTenWithZeros() {
		String padded = Helper.addPaddingZeros("655", 10);
		assertEquals("0000000655", padded);
	}
	
	@Test
	public void padToLengthEightWithZeros() {
		String padded = Helper.addPaddingZeros("655", 8);
		assertEquals("00000655", padded);
	}
	
	@Test
	public void noPaddingWhenLengthIsCorrect() {
		String padded = Helper.addPaddingZeros("0000000655", 10);
		assertEquals("0000000655", padded);
	}
}
