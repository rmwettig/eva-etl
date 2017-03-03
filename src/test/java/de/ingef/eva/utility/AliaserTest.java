package de.ingef.eva.utility;

import static org.junit.Assert.*;

import org.junit.Test;

public class AliaserTest {

	@Test
	public void testHasNext() {
		Aliaser a = new Aliaser(1);
		for(int i = 0; i < 26; i++)
			a.findNextAlias();
		
		assertFalse(a.hasNext());
	}

	@Test
	public void testFindNextAlias() {
		Aliaser a = new Aliaser(2);
		a.findNextAlias();
		String shouldBeB = a.findNextAlias();
		assertEquals("b", shouldBeB);
	}
	
	@Test
	public void testAliasOverflow() {
		Aliaser a = new Aliaser(2);
		for(int i = 0; i < 26; i++)
			a.findNextAlias();
		String shouldBeZA = a.findNextAlias();
		assertEquals("za", shouldBeZA);
	}

}
