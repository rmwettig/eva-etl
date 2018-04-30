package de.ingef.eva.models.sql;

import static org.junit.Assert.assertEquals;

import java.util.List;
import java.util.stream.IntStream;

import org.junit.Test;

import de.ingef.eva.configuration.export.sql.FixedYearSlice;

public class FixedYearSliceTest {

	@Test
	public void yearRangeIncludesStartAndEndYear() {
		List<Integer> calculatedYears = new FixedYearSlice("bezugsjahr", 2010, 2013).calculateYearRange();
		int[] expectedYears = new int[] {2010, 2011, 2012, 2013 };
		IntStream
			.range(0, expectedYears.length)
			.boxed()
			.forEach(index ->
				assertEquals(expectedYears[index], calculatedYears.get(index).intValue())
			);
	}

}
