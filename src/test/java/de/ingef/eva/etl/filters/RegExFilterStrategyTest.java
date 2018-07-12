package de.ingef.eva.etl.filters;

import static org.junit.Assert.*;

import org.junit.Test;

import de.ingef.eva.etl.RegExFilterStrategy;

public class RegExFilterStrategyTest {
	
	@Test
	public void validIfExactMatch() {
		RegExFilterStrategy strategy = new RegExFilterStrategy();
		String emptyOrTwoDigits = "^$|[0-9]{2}";
		strategy.setRegexPattern(emptyOrTwoDigits);
		strategy.initialize(null);
		assertTrue(strategy.isValid("23"));
	}
	
	@Test
	public void invalidIfPartialMatch() {
		RegExFilterStrategy strategy = new RegExFilterStrategy();
		String emptyOrTwoDigits = "^$|[0-9]{2}";
		strategy.setRegexPattern(emptyOrTwoDigits);
		strategy.initialize(null);
		assertFalse(strategy.isValid("{23"));
	}
	
	@Test
	public void invalidIfMismatch() {
		RegExFilterStrategy strategy = new RegExFilterStrategy();
		String emptyOrTwoDigits = "^$|[0-9]{2}";
		strategy.setRegexPattern(emptyOrTwoDigits);
		strategy.initialize(null);
		assertFalse(strategy.isValid("{"));
	}
}
