package de.ingef.eva.models.sql;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.time.LocalDate;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.Test;

import de.ingef.eva.configuration.export.sql.DynamicYearSlice;

public class DynamicYearSliceTest {

	@Test
	public void yearRangeIncludesCurrentYearAndThreePreviousYears() {
		int currentYear = LocalDate.now().getYear();
		int earliestYear = currentYear - 3;
		List<Integer> expectedYears = IntStream.rangeClosed(earliestYear, currentYear).boxed().collect(Collectors.toList());
		List<Integer> calculatedYears = new DynamicYearSlice("bezugsjahr", 3).calculateYearRange();
		assertFalse(calculatedYears.isEmpty());
		IntStream
			.range(0, expectedYears.size())
			.boxed()
			.forEach(index ->
				assertEquals(expectedYears.get(index), calculatedYears.get(index))
			);
	}
}
