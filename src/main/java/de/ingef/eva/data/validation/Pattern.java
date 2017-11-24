package de.ingef.eva.data.validation;

public final class Pattern {
	public static final String MATCH_CONTROLSYMBOLS = "[^\\p{Alnum}\\s();,./\\\\\u00e4\u00f6\u00fc\u00df\u00a7\u20ac-]";
	public static final String MATCH_TERMINAL_WHITESPACES = "^\\s+|\\s+$";
}
