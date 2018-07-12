package de.ingef.eva.etl.filters;

import static org.junit.Assert.*;

import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.Test;

import de.ingef.eva.etl.SetFilterStrategy;

public class SetFilterStrategyTest {

	@Test
	public void invalidIfNotInSet() {
		SetFilterStrategy strategy = new SetFilterStrategy(Stream.of("1").collect(Collectors.toSet()));
		assertFalse(strategy.isValid("2"));
	}
	
	@Test
	public void validIfInSet() {
		SetFilterStrategy strategy = new SetFilterStrategy(Stream.of("2").collect(Collectors.toSet()));
		assertTrue(strategy.isValid("2"));
	}

}
