package de.ingef.eva.processor;

import static org.junit.Assert.*;

import org.junit.Test;

public class RemovePatternTest {

	@Test
	public void testRemoveNonAlphanumericCharactersButKeepWhitespaces() {
		final Processor<String> p = new ReplacePattern(Pattern.MATCH_CONTROLSYMBOLS, "");
		final String text = "\t \u0000\u00029999;column 1\u0004\u0006\t ";
		final String result = p.process(text);
		assertEquals("\t 9999;column 1\t ", result);
	}
	
	@Test
	public void testRemoveBoundaryWhitespaces() {
		final Processor<String> p = new ReplacePattern(Pattern.MATCH_TERMINAL_WHITESPACES, "");
		final String text = "\t 9999;column 1\t ";
		final String result = p.process(text);
		assertEquals("9999;column 1", result);
	}
	
	@Test
	public void testDiacriticsMapper() {
		final String raw = "\u00e4\u00f6\u00fc\u00df\u00a7\u20ac";
		
		String result = new ReplacePattern(Pattern.MATCH_AE, "�").process(raw);
		result = new ReplacePattern(Pattern.MATCH_OE, "�").process(result);
		result = new ReplacePattern(Pattern.MATCH_UE, "�").process(result);
		result = new ReplacePattern(Pattern.MATCH_SZ, "�").process(result);
		result = new ReplacePattern(Pattern.MATCH_PARAGRAPH, "�").process(result);
		result = new ReplacePattern(Pattern.MATCH_EURO, "�").process(result);
		assertEquals("���ߧ�", result);
	}
	
	@Test
	public void testCleaningSpKEntry() {
		final String raw = "\t Mitglieder, weiblich;00000;-0000000000000000000;00000;00;normale Erkrankung;00;Besch\u00e4ftigte, mit mind. 6 W. EFZ (Info)\t \t";
		final String expected = "Mitglieder, weiblich;00000;-0000000000000000000;00000;00;normale Erkrankung;00;Besch\u00e4ftigte, mit mind. 6 W. EFZ (Info)";
		
		String result = new ReplacePattern(Pattern.MATCH_CONTROLSYMBOLS, "").process(raw);
		result = new ReplacePattern(Pattern.MATCH_TERMINAL_WHITESPACES,"").process(result);
		assertEquals(expected, result);
	}
	
	@Test
	public void testKeepSlashDateSeparators() {
		final String raw = "2010;YY/MM/DD;f";
		final String result = new ReplacePattern(Pattern.MATCH_CONTROLSYMBOLS, "").process(raw);
		
		assertEquals("2010;YY/MM/DD;f", result);
	}
	
	@Test
	public void testKeepBackslashDateSeparators() {
		final String raw = "2010;YY\\MM\\DD;f";
		final String result = new ReplacePattern(Pattern.MATCH_CONTROLSYMBOLS, "").process(raw);
		
		assertEquals("2010;YY\\MM\\DD;f", result);
	}
}
