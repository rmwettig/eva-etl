package de.ingef.eva.processor;

import static org.junit.Assert.*;

import org.junit.Test;

public class RemoveNewlineCharactersTest {

	@Test
	public void testRemovalOfNewlineCharacters() {
		RemoveNewlineCharacters p = new RemoveNewlineCharacters();
		
		//has control sequences
		StringBuilder s1 = new StringBuilder("ke\r\ny1");
		StringBuilder s2 = new StringBuilder("val\rue1");
		
		//control sequences removed
		assertEquals("key1", p.process(s1).toString());
		assertEquals("value1", p.process(s2).toString());

	}
}
