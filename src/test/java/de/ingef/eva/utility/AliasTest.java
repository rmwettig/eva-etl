package de.ingef.eva.utility;

import static org.junit.Assert.*;

import org.junit.Test;

public class AliasTest {

	@Test
	public void testHasNext() {
		Alias a = new Alias(1);
		for(int i = 0; i < 26; i++)
			a.findNextAlias();
		
		assertFalse(a.hasNext());
	}

	@Test
	public void testFindNextAlias() {
		Alias a = new Alias(2);
		a.findNextAlias();
		String shouldBeB = a.findNextAlias();
		assertEquals("b", shouldBeB);
	}
	
	@Test
	public void testAliasOverflow() {
		Alias a = new Alias(2);
		for(int i = 0; i < 26; i++)
			a.findNextAlias();
		String shouldBeZA = a.findNextAlias();
		assertEquals("za", shouldBeZA);
	}

}
