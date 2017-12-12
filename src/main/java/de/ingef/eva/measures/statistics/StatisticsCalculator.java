package de.ingef.eva.measures.statistics;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

import de.ingef.eva.measures.cci.Quarter;
import de.ingef.eva.utility.QuarterCount;

public class StatisticsCalculator {
	
	public StatisticsEntry calculateOverviewStatistics(DataSlice slice, List<QuarterCount> absoluteNumbers, int lowerYearBound, int inclusiveUpperYearBound) {
		List<QuarterCount> paddedAbsoluteNumbers = padDataToYearBounds(absoluteNumbers, lowerYearBound, inclusiveUpperYearBound);
		return new StatisticsEntry(slice.getLabel(), slice.getLabel(), calculateRatios(paddedAbsoluteNumbers));
	}
		
	private List<QuarterCount> calculateRatios(List<QuarterCount> paddedAbsoluteNumbers) {
		List<QuarterCount> ratios = createBaselineQuarters(paddedAbsoluteNumbers);
		for(int i = 4; i < paddedAbsoluteNumbers.size(); i++) {
			QuarterCount currentQuarterCount = paddedAbsoluteNumbers.get(i);
			QuarterCount previousYearQuarterCount = paddedAbsoluteNumbers.get(i - 4);
			Quarter quarter = currentQuarterCount.getQuarter();
			QuarterCount newQc = new QuarterCount(new Quarter(quarter.getYear(), quarter.getQuarter()), currentQuarterCount.getCount());
			if(previousYearQuarterCount.getCount() != 0)
				newQc.setChangeRatio(currentQuarterCount.getCount() / (double) previousYearQuarterCount.getCount());
			
			ratios.add(newQc);
		}
		
		return ratios;
	}

	private List<QuarterCount> createBaselineQuarters(List<QuarterCount> paddedAbsoluteNumbers) {
		List<QuarterCount> ratios = new ArrayList<>(paddedAbsoluteNumbers.size());
		//first four quarters have no ratio
		IntStream
			.range(0, 4)
			.forEach(i -> {
				QuarterCount qc = paddedAbsoluteNumbers.get(i);
				Quarter q = qc.getQuarter();
				ratios.add(new QuarterCount(new Quarter(q.getYear(), q.getQuarter()), qc.getCount()));
			});
		return ratios;
	}

	private List<QuarterCount> padDataToYearBounds(List<QuarterCount> absoluteNumbers, int lowerYearBound, int inclusiveUpperYearBound) {
		List<QuarterCount> paddedData = new ArrayList<>(absoluteNumbers.size());
		Quarter expectedQuarter = new Quarter(lowerYearBound, 1);
		int quarterCountIndex = 0;
		int dataPointCount = absoluteNumbers.size();
		while(expectedQuarter.getQuarter() <= 4 && expectedQuarter.getYear() <= inclusiveUpperYearBound) {
			//as long as there is data that the expectation can be compared with
			if(quarterCountIndex < dataPointCount) {
				QuarterCount foundQuarter = absoluteNumbers.get(quarterCountIndex++);
				int distance = Quarter.distanceInQuarters(expectedQuarter, foundQuarter.getQuarter());
				//if the expected quarter is present save it
				if(distance == 0) {
					paddedData.add(foundQuarter);
					expectedQuarter = expectedQuarter.increment();
				} else {
					//otherwise fill gaps with zero entries
					for(int i = 0; i < distance; i++) {
						paddedData.add(new QuarterCount(expectedQuarter, 0));
				 		expectedQuarter = expectedQuarter.increment();
					}
				}
			//no more data is present so just add zero entries
			} else {
				paddedData.add(new QuarterCount(expectedQuarter, 0));
				expectedQuarter = expectedQuarter.increment();
			}
		}
				
		return paddedData;
	}
	
}
