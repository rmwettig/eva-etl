package de.ingef.eva.measures.statistics;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import de.ingef.eva.measures.cci.Quarter;

public class QuarterTest {

	@Test
	public void increasesQuarterByOne() {
		Quarter q = new Quarter(2010, 1).increment();
		assertNotNull(q);
		assertEquals(2010, q.getYear());
		assertEquals(2, q.getQuarter());
	}
	
	@Test
	public void increasesYearByOneAfterLastQuarter() {
		Quarter q = new Quarter(2010, 4).increment();
		assertNotNull(q);
		assertEquals(2011, q.getYear());
		assertEquals(1, q.getQuarter());
	}

	@Test
	public void identityIsZero() {
		Quarter q = new Quarter(2010, 1);
		int distance = Quarter.distanceInQuarters(q, q);
		assertEquals(0, distance);
	}
	
	@Test
	public void withinYearDistanceEqualsQuarterDistance() {
		Quarter a = new Quarter(2010, 1);
		Quarter b = new Quarter(2010, 3);
		int distance = Quarter.distanceInQuarters(a, b);
		assertEquals(2, distance);
	}
	
	@Test
	public void acrossYearDistanceEqualsQuarterDistancePlusYearDifferenceInQuarters() {
		Quarter a = new Quarter(2010, 1);
		Quarter b = new Quarter(2011, 3);
		int distance = Quarter.distanceInQuarters(a, b);
		assertEquals(6, distance);
	}
	
	@Test
	public void acrossYearDistanceEqualsQuarterDistance() {
		Quarter a = new Quarter(2010, 3);
		Quarter b = new Quarter(2011, 1);
		int distance = Quarter.distanceInQuarters(a, b);
		assertEquals(2, distance);
	}
	
	@Test
	public void mustBeEqual() {
		Quarter a = new Quarter(2010, 3);
		Quarter b = new Quarter(2010, 3);
		assertTrue(a.equals(b));
	}
	
	@Test
	public void mustBeUnequalIfYearDiffers() {
		Quarter a = new Quarter(2010, 3);
		Quarter b = new Quarter(2011, 3);
		assertFalse(a.equals(b));
	}
	
	@Test
	public void mustBeUnequalIfQuarterDiffers() {
		Quarter a = new Quarter(2010, 3);
		Quarter b = new Quarter(2010, 4);
		assertFalse(a.equals(b));
	}
}
