package de.ingef.eva.measures.statistics;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import de.ingef.eva.measures.cci.Quarter;
import de.ingef.eva.utility.QuarterCount;

public class RegionalStatisticCalculatorTest {
	
	@Test
	public void ratioIsZeroIfNoDataInPreviousYearQuarter() {
		String kv = "01";
		String kvName = "Schleswig-Holstein";
		Quarter referenceQuarter = new Quarter(2017, 3);
		List<QuarterCount> absoluteNumbers = Arrays.asList(
				new QuarterCount(referenceQuarter, 300),
				new QuarterCount(new Quarter(2016, 3), 0)
		);
		StatisticsEntry entry = new RegionalStatisticCalculator().calculateRegionalStatistic(referenceQuarter, absoluteNumbers, kv, kvName);
		assertNotNull(entry);
		assertEquals(kv, entry.getIdentifier());
		assertEquals(kvName, entry.getLabel());
		List<QuarterCount> counts = entry.getDataCount();
		assertNotNull(counts);
		assertEquals(1, counts.size());
		QuarterCount count = counts.get(0);
		assertNotNull(count);
		Quarter q = count.getQuarter();
		assertNotNull(q);
		assertEquals(2017, q.getYear());
		assertEquals(3, q.getQuarter());
		assertEquals(0, count.getChangeRatio(), 0.0001);
	}
	
	@Test
	public void ratioIsZeroIfPreviousYearQuarterIsMissing() {
		String kv = "01";
		String kvName = "Schleswig-Holstein";
		Quarter referenceQuarter = new Quarter(2017, 3);
		List<QuarterCount> absoluteNumbers = Arrays.asList(
				new QuarterCount(referenceQuarter, 300)
		);
		StatisticsEntry entry = new RegionalStatisticCalculator().calculateRegionalStatistic(referenceQuarter, absoluteNumbers, kv, kvName);
		assertNotNull(entry);
		assertEquals(kv, entry.getIdentifier());
		assertEquals(kvName, entry.getLabel());
		List<QuarterCount> counts = entry.getDataCount();
		assertNotNull(counts);
		assertEquals(1, counts.size());
		QuarterCount count = counts.get(0);
		assertNotNull(count);
		Quarter q = count.getQuarter();
		assertNotNull(q);
		assertEquals(referenceQuarter.getYear(), q.getYear());
		assertEquals(referenceQuarter.getQuarter(), q.getQuarter());
		assertEquals(0, count.getChangeRatio(), 0.0001);
	}
	
	@Test
	public void ratioIsZeroIfReferenceQuarterIsMissing() {
		String kv = "01";
		String kvName = "Schleswig-Holstein";
		Quarter referenceQuarter = new Quarter(2017, 3);
		List<QuarterCount> absoluteNumbers = Arrays.asList(
				new QuarterCount(new Quarter(2016, 3), 300)
		);
		StatisticsEntry entry = new RegionalStatisticCalculator().calculateRegionalStatistic(referenceQuarter, absoluteNumbers, kv, kvName);
		assertNotNull(entry);
		assertEquals(kv, entry.getIdentifier());
		assertEquals(kvName, entry.getLabel());
		List<QuarterCount> counts = entry.getDataCount();
		assertNotNull(counts);
		assertEquals(1, counts.size());
		QuarterCount count = counts.get(0);
		assertNotNull(count);
		Quarter q = count.getQuarter();
		assertNotNull(q);
		assertEquals(referenceQuarter.getYear(), q.getYear());
		assertEquals(referenceQuarter.getQuarter(), q.getQuarter());
		assertEquals(0, count.getChangeRatio(), 0.0001);
	}
	
	@Test
	public void ratioEqualsReferenceQuarterCountDividedByPreviousYearCount() {
		String kv = "01";
		String kvName = "Schleswig-Holstein";
		Quarter referenceQuarter = new Quarter(2017, 3);
		List<QuarterCount> absoluteNumbers = Arrays.asList(
				new QuarterCount(new Quarter(2017, 3), 300),
				new QuarterCount(new Quarter(2016, 3), 200)
		);
		StatisticsEntry entry = new RegionalStatisticCalculator().calculateRegionalStatistic(referenceQuarter, absoluteNumbers, kv, kvName);
		assertNotNull(entry);
		assertEquals(kv, entry.getIdentifier());
		assertEquals(kvName, entry.getLabel());
		List<QuarterCount> counts = entry.getDataCount();
		assertNotNull(counts);
		assertEquals(1, counts.size());
		QuarterCount count = counts.get(0);
		assertNotNull(count);
		Quarter q = count.getQuarter();
		assertNotNull(q);
		assertEquals(2017, q.getYear());
		assertEquals(3, q.getQuarter());
		assertEquals(300/(double)200, count.getChangeRatio(), 0.0001);
	}
	
	@Test
	public void ratioIsZeroIfPrevYearEntryQuarterDoesNotMatch() {
		String kv = "01";
		String kvName = "Schleswig-Holstein";
		Quarter referenceQuarter = new Quarter(2017, 3);
		List<QuarterCount> absoluteNumbers = Arrays.asList(
				new QuarterCount(referenceQuarter, 300),
				new QuarterCount(new Quarter(2016, 1), 200)
		);
		StatisticsEntry entry = new RegionalStatisticCalculator().calculateRegionalStatistic(referenceQuarter, absoluteNumbers, kv, kvName);
		assertNotNull(entry);
		assertEquals(kv, entry.getIdentifier());
		assertEquals(kvName, entry.getLabel());
		List<QuarterCount> counts = entry.getDataCount();
		assertNotNull(counts);
		assertEquals(1, counts.size());
		QuarterCount count = counts.get(0);
		assertNotNull(count);
		Quarter q = count.getQuarter();
		assertNotNull(q);
		assertEquals(2017, q.getYear());
		assertEquals(3, q.getQuarter());
		assertEquals(0, count.getChangeRatio(), 0.0001);
	}

	@Test
	public void ratioIsZeroIfPrevYearEntryYearDoesNotMatch() {
		String kv = "01";
		String kvName = "Schleswig-Holstein";
		Quarter referenceQuarter = new Quarter(2017, 3);
		List<QuarterCount> absoluteNumbers = Arrays.asList(
				new QuarterCount(referenceQuarter, 300),
				new QuarterCount(new Quarter(2015, 3), 200)
		);
		StatisticsEntry entry = new RegionalStatisticCalculator().calculateRegionalStatistic(referenceQuarter, absoluteNumbers, kv, kvName);
		assertNotNull(entry);
		assertEquals(kv, entry.getIdentifier());
		assertEquals(kvName, entry.getLabel());
		List<QuarterCount> counts = entry.getDataCount();
		assertNotNull(counts);
		assertEquals(1, counts.size());
		QuarterCount count = counts.get(0);
		assertNotNull(count);
		Quarter q = count.getQuarter();
		assertNotNull(q);
		assertEquals(2017, q.getYear());
		assertEquals(3, q.getQuarter());
		assertEquals(0, count.getChangeRatio(), 0.0001);
	}
	
	@Test
	public void searchesForReferenceQuarterIfOrderIsWrong() {
		String kv = "01";
		String kvName = "Schleswig-Holstein";
		Quarter referenceQuarter = new Quarter(2017, 3);
		List<QuarterCount> absoluteNumbers = Arrays.asList(
				new QuarterCount(new Quarter(2016, 3), 200),
				new QuarterCount(referenceQuarter, 300)
		);
		StatisticsEntry entry = new RegionalStatisticCalculator().calculateRegionalStatistic(referenceQuarter, absoluteNumbers, kv, kvName);
		assertNotNull(entry);
		assertEquals(kv, entry.getIdentifier());
		assertEquals(kvName, entry.getLabel());
		List<QuarterCount> counts = entry.getDataCount();
		assertNotNull(counts);
		assertEquals(1, counts.size());
		QuarterCount count = counts.get(0);
		assertNotNull(count);
		Quarter q = count.getQuarter();
		assertNotNull(q);
		assertEquals(2017, q.getYear());
		assertEquals(3, q.getQuarter());
		assertEquals(300/(double)200, count.getChangeRatio(), 0.0001);
	}
}
