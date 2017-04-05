package de.ingef.eva.processor;

import static org.junit.Assert.*;

import org.junit.Test;

public class RemovePatternTest {

	@Test
	public void testRemoveNonAlphanumericCharactersButKeepWhitespaces() {
		final Processor<String> p = new ReplacePattern("[^\\p{Alnum}\\s;.-]", "");
		final String text = "\t \u0000\u00029999;column 1\u0004\u0006\t ";
		final String result = p.process(text);
		assertEquals("\t 9999;column 1\t ", result);
	}
	
	@Test
	public void testRemoveBoundaryWhitespaces() {
		final Processor<String> p = new ReplacePattern("^\\s+|\\s+$", "");
		final String text = "\t 9999;column 1\t ";
		final String result = p.process(text);
		assertEquals("9999;column 1", result);
	}
	
	@Test
	public void testDiacriticsMapper() {
		final String raw = "\u00e4\u00f6\u00fc\u00df\u00a7\u20ac";
		String result = new ReplacePattern("\u00e4", "ä").process(raw);
		result = new ReplacePattern("\u00e4", "ä").process(raw);
		result = new ReplacePattern("\u00f6", "ö").process(result);
		result = new ReplacePattern("\u00fc", "ü").process(result);
		result = new ReplacePattern("\u00df", "ß").process(result);
		result = new ReplacePattern("\u00a7", "§").process(result);
		result = new ReplacePattern("\u20ac", "€").process(result);
		assertEquals("äöüß§€", result);
	}
	
	@Test
	public void testCleaningSpKEntry() {
		final String raw = "\t Mitglieder, weiblich;00000;-0000000000000000000;00000;00;normale Erkrankung;00;Besch\u00e4ftigte, mit mind. 6 W. EFZ\t \t";
		final String expected = "Mitglieder, weiblich;00000;-0000000000000000000;00000;00;normale Erkrankung;00;Besch\u00e4ftigte, mit mind. 6 W. EFZ";
		
		String result = new ReplacePattern("[^\\p{Alnum}\\s;\\-,.\u00e4]", "").process(raw);
		result = new ReplacePattern("^\\s+|\\s+$","").process(result);
		assertEquals(expected, result);
	}
}
