package de.ingef.eva.processor;

public final class Pattern {
	public static final String MATCH_CONTROLSYMBOLS = "[^\\p{Alnum}\\s();,./\\\\\u00e4\u00f6\u00fc\u00df\u00a7\u20ac-]";
	public static final String MATCH_TERMINAL_WHITESPACES = "^\\s+|\\s+$";
	public static final String MATCH_AE = "\u00e4";
	public static final String MATCH_OE = "\u00f6";
	public static final String MATCH_UE = "\u00fc";
	public static final String MATCH_SZ = "\u00df";
	public static final String MATCH_PARAGRAPH = "\u00a7";
	public static final String MATCH_EURO = "\u20ac";
}
