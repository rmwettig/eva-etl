package de.ingef.eva.processor;

import static org.junit.Assert.*;

import org.junit.Test;

import de.ingef.eva.constant.TeradataColumnType;

public class RemovePatternTest {

	@Test
	public void testRemoveNonAlphanumericCharactersButKeepWhitespaces() {
		final Processor<String> p = new ReplacePattern(TeradataColumnType.ANY, Pattern.MATCH_CONTROLSYMBOLS, "");
		final String text = "\t \u0000\u00029999;column 1\u0004\u0006\t ";
		final String result = p.process(text);
		assertEquals("\t 9999;column 1\t ", result);
	}
	
	@Test
	public void testRemoveBoundaryWhitespaces() {
		final Processor<String> p = new ReplacePattern(TeradataColumnType.ANY, Pattern.MATCH_TERMINAL_WHITESPACES, "");
		final String text = "\t 9999;column 1\t ";
		final String result = p.process(text);
		assertEquals("9999;column 1", result);
	}
	
	@Test
	public void testDiacriticsMapper() {
		final String raw = "\u00e4\u00f6\u00fc\u00df\u00a7\u20ac";
		
		String result = new ReplacePattern(TeradataColumnType.ANY, Pattern.MATCH_AE, "ä").process(raw);
		result = new ReplacePattern(TeradataColumnType.ANY, Pattern.MATCH_OE, "ö").process(result);
		result = new ReplacePattern(TeradataColumnType.ANY, Pattern.MATCH_UE, "ü").process(result);
		result = new ReplacePattern(TeradataColumnType.ANY, Pattern.MATCH_SZ, "ß").process(result);
		result = new ReplacePattern(TeradataColumnType.ANY, Pattern.MATCH_PARAGRAPH, "§").process(result);
		result = new ReplacePattern(TeradataColumnType.ANY, Pattern.MATCH_EURO, "€").process(result);
		assertEquals("äöüß§€", result);
	}
	
	@Test
	public void testCleaningSpKEntry() {
		final String raw = "\t Mitglieder, weiblich;00000;-0000000000000000000;00000;00;normale Erkrankung;00;Besch\u00e4ftigte, mit mind. 6 W. EFZ (Info)\t \t";
		final String expected = "Mitglieder, weiblich;00000;-0000000000000000000;00000;00;normale Erkrankung;00;Besch\u00e4ftigte, mit mind. 6 W. EFZ (Info)";
		
		String result = new ReplacePattern(TeradataColumnType.ANY, Pattern.MATCH_CONTROLSYMBOLS, "").process(raw);
		result = new ReplacePattern(TeradataColumnType.ANY, Pattern.MATCH_TERMINAL_WHITESPACES,"").process(result);
		assertEquals(expected, result);
	}
	
	@Test
	public void testKeepSlashDateSeparators() {
		final String raw = "2010;YY/MM/DD;f";
		final String result = new ReplacePattern(TeradataColumnType.ANY, Pattern.MATCH_CONTROLSYMBOLS, "").process(raw);
		
		assertEquals("2010;YY/MM/DD;f", result);
	}
	
	@Test
	public void testKeepBackslashDateSeparators() {
		final String raw = "2010;YY\\MM\\DD;f";
		final String result = new ReplacePattern(TeradataColumnType.ANY, Pattern.MATCH_CONTROLSYMBOLS, "").process(raw);
		
		assertEquals("2010;YY\\MM\\DD;f", result);
	}
	
	@Test
	public void testRemoveWhitespacesInScientificFloats() {
		final String raw = "3.4000e 004";
		final String result = new ReplacePattern(TeradataColumnType.ANY, Pattern.MATCH_INVALID_SCIENTIFIC_FLOAT_CHARS, "").process(raw);
		
		assertEquals("3.4000e004", result);
	}
	
	@Test
	public void testKeepPlusSignInScientificFloats() {
		final String raw = "3.4000e+004";
		final String result = new ReplacePattern(TeradataColumnType.ANY, Pattern.MATCH_INVALID_SCIENTIFIC_FLOAT_CHARS, "").process(raw);
		
		assertEquals("3.4000e+004", result);
	}
	
	@Test
	public void testRemoveLettersInScientificFloats() {
		final String raw = "3.4I0a0ö0e+004";
		final String result = new ReplacePattern(TeradataColumnType.ANY, Pattern.MATCH_INVALID_SCIENTIFIC_FLOAT_CHARS, "").process(raw);
		
		assertEquals("3.4000e+004", result);
	}
}
