package de.ingef.eva.measures.statistics;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import de.ingef.eva.measures.cci.Quarter;
import de.ingef.eva.utility.QuarterCount;

public class StatisticsCalculatorTest {

	@Test
	public void calculateChangeRatioCorrectly() {
		List<QuarterCount> absoluteNumbers = Arrays.asList(
			new QuarterCount(new Quarter(2010, 1), 100),
			new QuarterCount(new Quarter(2010, 2), 200),
			new QuarterCount(new Quarter(2010, 3), 300),
			new QuarterCount(new Quarter(2010, 4), 400),
			new QuarterCount(new Quarter(2011, 1), 200),
			new QuarterCount(new Quarter(2011, 2), 400),
			new QuarterCount(new Quarter(2011, 3), 1500),
			new QuarterCount(new Quarter(2011, 4), 1200)
		);
		
		StatisticsEntry result = new StatisticsCalculator().calculateOverviewStatistics(DataSlice.AM_EVO, absoluteNumbers, 2010, 2011);
		assertNotNull(result);
		assertEquals(DataSlice.AM_EVO.getLabel(), result.getIdentifier());
		assertEquals(DataSlice.AM_EVO.getLabel(), result.getLabel());
		List<QuarterCount> ratios = result.getDataCount();
		assertNotNull(ratios);
		assertEquals(absoluteNumbers.size(), ratios.size());
		
		QuarterCount qc = ratios.get(0);
		assertEquals(0.0, qc.getChangeRatio(), 0.0001);
		Quarter q = qc.getQuarter();
		assertNotNull(q);
		assertEquals(2010, q.getYear());
		assertEquals(1, q.getQuarter());
		
		qc = ratios.get(1);
		assertEquals(0.0, qc.getChangeRatio(), 0.0001);
		q = qc.getQuarter();
		assertNotNull(q);
		assertEquals(2010, q.getYear());
		assertEquals(2, q.getQuarter());
		
		qc = ratios.get(2);
		assertEquals(0.0, qc.getChangeRatio(), 0.0001);
		q = qc.getQuarter();
		assertNotNull(q);
		assertEquals(2010, q.getYear());
		assertEquals(3, q.getQuarter());
		
		qc = ratios.get(3);
		assertEquals(0.0, qc.getChangeRatio(), 0.0001);
		q = qc.getQuarter();
		assertNotNull(q);
		assertEquals(2010, q.getYear());
		assertEquals(4, q.getQuarter());
		
		qc = ratios.get(4);
		assertEquals(2.0, qc.getChangeRatio(), 0.0001);
		q = qc.getQuarter();
		assertNotNull(q);
		assertEquals(2011, q.getYear());
		assertEquals(1, q.getQuarter());
		
		qc = ratios.get(5);
		assertEquals(2.0, qc.getChangeRatio(), 0.0001);
		q = qc.getQuarter();
		assertNotNull(q);
		assertEquals(2011, q.getYear());
		assertEquals(2, q.getQuarter());
		
		qc = ratios.get(6);
		assertEquals(5.0, qc.getChangeRatio(), 0.0001);
		q = qc.getQuarter();
		assertNotNull(q);
		assertEquals(2011, q.getYear());
		assertEquals(3, q.getQuarter());
		
		qc = ratios.get(7);
		assertEquals(3.0, qc.getChangeRatio(), 0.0001);
		q = qc.getQuarter();
		assertNotNull(q);
		assertEquals(2011, q.getYear());
		assertEquals(4, q.getQuarter());
	}
	
	@Test
	public void fillMissingDataWithZero() {
		List<QuarterCount> absoluteNumbers = Arrays.asList(
			new QuarterCount(new Quarter(2010, 1), 100)
		);
		
		StatisticsEntry result = new StatisticsCalculator().calculateOverviewStatistics(DataSlice.AM_EVO, absoluteNumbers, 2010, 2010);
		assertNotNull(result);
		assertEquals(DataSlice.AM_EVO.getLabel(), result.getIdentifier());
		assertEquals(DataSlice.AM_EVO.getLabel(), result.getLabel());
		List<QuarterCount> ratios = result.getDataCount();
		assertNotNull(ratios);
		assertEquals(4, ratios.size());
		
		QuarterCount qc = ratios.get(0);
		assertEquals(0.0, qc.getChangeRatio(), 0.0001);
		Quarter q = qc.getQuarter();
		assertNotNull(q);
		assertEquals(2010, q.getYear());
		assertEquals(1, q.getQuarter());
		
		qc = ratios.get(1);
		assertEquals(0.0, qc.getChangeRatio(), 0.0001);
		q = qc.getQuarter();
		assertNotNull(q);
		assertEquals(2010, q.getYear());
		assertEquals(2, q.getQuarter());
		
		qc = ratios.get(2);
		assertEquals(0.0, qc.getChangeRatio(), 0.0001);
		q = qc.getQuarter();
		assertNotNull(q);
		assertEquals(2010, q.getYear());
		assertEquals(3, q.getQuarter());
		
		qc = ratios.get(3);
		assertEquals(0.0, qc.getChangeRatio(), 0.0001);
		q = qc.getQuarter();
		assertNotNull(q);
		assertEquals(2010, q.getYear());
		assertEquals(4, q.getQuarter());
	}
	
	@Test
	public void ratioIsZeroIfPriorYearCountIsZero() {
		List<QuarterCount> absoluteNumbers = Arrays.asList(
				new QuarterCount(new Quarter(2010, 1), 0),
				new QuarterCount(new Quarter(2010, 2), 0),
				new QuarterCount(new Quarter(2010, 3), 0),
				new QuarterCount(new Quarter(2010, 4), 0),
				new QuarterCount(new Quarter(2011, 1), 200),
				new QuarterCount(new Quarter(2011, 2), 400),
				new QuarterCount(new Quarter(2011, 3), 1500),
				new QuarterCount(new Quarter(2011, 4), 1200)
			);
			
			StatisticsEntry result = new StatisticsCalculator().calculateOverviewStatistics(DataSlice.AM_EVO, absoluteNumbers, 2010, 2011);
			assertNotNull(result);
			assertEquals(DataSlice.AM_EVO.getLabel(), result.getIdentifier());
			assertEquals(DataSlice.AM_EVO.getLabel(), result.getLabel());
			List<QuarterCount> ratios = result.getDataCount();
			assertNotNull(ratios);
			assertEquals(8, ratios.size());
			
			QuarterCount qc = ratios.get(0);
			assertEquals(0.0, qc.getChangeRatio(), 0.0001);
			Quarter q = qc.getQuarter();
			assertNotNull(q);
			assertEquals(2010, q.getYear());
			assertEquals(1, q.getQuarter());
			
			qc = ratios.get(1);
			assertEquals(0.0, qc.getChangeRatio(), 0.0001);
			q = qc.getQuarter();
			assertNotNull(q);
			assertEquals(2010, q.getYear());
			assertEquals(2, q.getQuarter());
			
			qc = ratios.get(2);
			assertEquals(0.0, qc.getChangeRatio(), 0.0001);
			q = qc.getQuarter();
			assertNotNull(q);
			assertEquals(2010, q.getYear());
			assertEquals(3, q.getQuarter());
			
			qc = ratios.get(3);
			assertEquals(0.0, qc.getChangeRatio(), 0.0001);
			q = qc.getQuarter();
			assertNotNull(q);
			assertEquals(2010, q.getYear());
			assertEquals(4, q.getQuarter());
			
			qc = ratios.get(4);
			assertEquals(0.0, qc.getChangeRatio(), 0.0001);
			q = qc.getQuarter();
			assertNotNull(q);
			assertEquals(2011, q.getYear());
			assertEquals(1, q.getQuarter());
			
			qc = ratios.get(5);
			assertEquals(0.0, qc.getChangeRatio(), 0.0001);
			q = qc.getQuarter();
			assertNotNull(q);
			assertEquals(2011, q.getYear());
			assertEquals(2, q.getQuarter());
			
			qc = ratios.get(6);
			assertEquals(0.0, qc.getChangeRatio(), 0.0001);
			q = qc.getQuarter();
			assertNotNull(q);
			assertEquals(2011, q.getYear());
			assertEquals(3, q.getQuarter());
			
			qc = ratios.get(7);
			assertEquals(0.0, qc.getChangeRatio(), 0.0001);
			q = qc.getQuarter();
			assertNotNull(q);
			assertEquals(2011, q.getYear());
			assertEquals(4, q.getQuarter());
	}
}
