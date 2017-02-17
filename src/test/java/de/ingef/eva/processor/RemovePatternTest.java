package de.ingef.eva.processor;

import static org.junit.Assert.*;

import org.junit.Test;

public class RemovePatternTest {

	@Test
	public void testRemoveNonAlphanumericCharactersButKeepWhitespaces()
	{
		final Processor<String> p = new RemovePattern("[^\\p{Alnum}\\s;]");
		final String text = "\t \u0000\u00029999;column 1\u0004\u0006\t ";
		final String result = p.process(text);
		assertEquals("\t 9999;column 1\t ", result);
	}
	
	@Test
	public void testRemoveBoundaryWhitespaces()
	{
		final Processor<String> p = new RemovePattern("^\\s+|\\s+$");
		final String text = "\t 9999;column 1\t ";
		final String result = p.process(text);
		assertEquals("9999;column 1", result);
	}
}
