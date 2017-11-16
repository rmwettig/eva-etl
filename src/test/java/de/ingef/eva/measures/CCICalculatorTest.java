package de.ingef.eva.measures;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

import org.junit.Test;

import de.ingef.eva.measures.cci.CCICalculator;
import de.ingef.eva.measures.cci.Quarter;
import de.ingef.eva.measures.cci.QuarterEntry;
import de.ingef.eva.measures.cci.QuarterScoreResult;

public class CCICalculatorTest {
	
	@Test
	public void takeMaximumWithinDiseaseClass() {
		List<QuarterEntry> data = new ArrayList<>(7);
		data.add(new QuarterEntry(1, 2010, "A", "a", 1, "108036123"));
		data.add(new QuarterEntry(2, 2010, "A", "a", 2, "108036123"));
		data.add(new QuarterEntry(3, 2010, "A", "a", 3, "108036123"));
		data.add(new QuarterEntry(4, 2010, "A", "a", 4, "108036123"));
		
		QuarterScoreResult result = new CCICalculator().calculateSlidingWindow(data, 2010, 1);
		assertNotNull(result);
		
		Quarter start = result.getStart();
		assertNotNull(start);
		assertEquals(1, start.getQuarter());
		assertEquals(2010, start.getYear());
		
		Quarter end = result.getEnd();
		assertNotNull(end);
		assertEquals(4, end.getQuarter());
		assertEquals(2010, end.getYear());
		
		assertEquals(4, result.getWeight());
	}
	
	@Test
	public void takeSumBetweenDiseaseClasses() {
		List<QuarterEntry> data = new ArrayList<>(7);
		data.add(new QuarterEntry(1, 2010, "A", "a", 1, "108036123"));
		data.add(new QuarterEntry(2, 2010, "A", "a", 1, "108036123"));
		data.add(new QuarterEntry(3, 2010, "A", "b", 2, "108036123"));
		data.add(new QuarterEntry(4, 2010, "A", "b", 2, "108036123"));
		
		QuarterScoreResult result = new CCICalculator().calculateSlidingWindow(data, 2010, 1);
		assertNotNull(result);
		
		Quarter start = result.getStart();
		assertNotNull(start);
		assertEquals(1, start.getQuarter());
		assertEquals(2010, start.getYear());
		
		Quarter end = result.getEnd();
		assertNotNull(end);
		assertEquals(4, end.getQuarter());
		assertEquals(2010, end.getYear());
		
		assertEquals(3, result.getWeight());
	}
	
	@Test
	public void ignoreScoresBeyondUpperQuarterLimit() {
		List<QuarterEntry> data = new ArrayList<>(7);
		data.add(new QuarterEntry(1, 2010, "A", "a", 2, "108036123"));
		data.add(new QuarterEntry(2, 2010, "A", "b", 2, "108036123"));
		data.add(new QuarterEntry(3, 2010, "A", "a", 1, "108036123"));
		data.add(new QuarterEntry(4, 2010, "A", "b", 1, "108036123"));
		data.add(new QuarterEntry(1, 2011, "A", "b", 3, "108036123"));
		
		QuarterScoreResult result = new CCICalculator().calculateSlidingWindow(data, 2010, 1);
		assertNotNull(result);
		
		Quarter start = result.getStart();
		assertNotNull(start);
		assertEquals(1, start.getQuarter());
		assertEquals(2010, start.getYear());
		
		Quarter end = result.getEnd();
		assertNotNull(end);
		assertEquals(4, end.getQuarter());
		assertEquals(2010, end.getYear());
		
		assertEquals(4, result.getWeight());
	}
	
	@Test
	public void truncateQuarterLimitIfEndIsInFuture() {
		List<QuarterEntry> data = new ArrayList<>(1);
		final int currentYear = Calendar.getInstance().get(Calendar.YEAR);
		data.add(new QuarterEntry(3, currentYear, "A", "a", 2, "108036123"));
		data.add(new QuarterEntry(4, currentYear, "A", "a", 2, "108036123"));
		data.add(new QuarterEntry(1, currentYear + 1, "B", "b", 3, "108036123"));
		data.add(new QuarterEntry(2, currentYear + 1, "B", "b", 3, "108036123"));
		
		QuarterScoreResult result = new CCICalculator().calculateSlidingWindow(data, currentYear, 3);
		assertNotNull(result);
		
		Quarter start = result.getStart();
		assertNotNull(start);
		assertEquals(currentYear, start.getYear());
		assertEquals(3, start.getQuarter());
		
		Quarter end = result.getEnd();
		assertNotNull(end);
		assertEquals(currentYear, end.getYear());
		assertEquals(4, end.getQuarter());
		
		assertEquals(2, result.getWeight());
	}
	
	@Test
	public void considerMissingQuartersAsZero() {
		List<QuarterEntry> data = new ArrayList<>(7);
		data.add(new QuarterEntry(1, 2010, "A", "a", 2, "108036123"));
		data.add(new QuarterEntry(4, 2010, "A", "b", 1, "108036123"));
		
		QuarterScoreResult result = new CCICalculator().calculateSlidingWindow(data, 2010, 1);
		assertNotNull(result);
		
		Quarter start = result.getStart();
		assertNotNull(start);
		assertEquals(2010, start.getYear());
		assertEquals(1, start.getQuarter());
		
		Quarter end = result.getEnd();
		assertNotNull(end);
		assertEquals(2010, end.getYear());
		assertEquals(4, end.getQuarter());
		
		assertEquals(3, result.getWeight());
	}
}
