package de.ingef.eva.measures;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

/**
 * Aggregates icd class weights for a pid.
 * It assumes that entries are added in order, i.e. data must begin with smallest year Q1 and end with largest year Q4
 * 
 * @author Martin Wettig
 *
 */
public class CharlsonScore {
	
	@Getter
	@AllArgsConstructor
	private final class Entry {
		private final int year;
		private final int quarter;
		private final String diseaseClass;
		private int weight = 0;
		
		public void updateWeight(int newWeight) {
			weight = Math.max(weight, newWeight);
		}
		
		@Override
		public String toString() {
			return String.format("[%dQ%d class: %s; %d]", year, quarter, diseaseClass, weight);
		}
	}
	
	@Getter
	@RequiredArgsConstructor
	private final class SlidingWindow {
		private final int startYear;
		private final int startQuarter;
		private final int endYear;
		private final int endQuarter;
		private final int score;
		
		@Override
		public String toString() {
			return String.format("[%dQ%d; %dQ%d; %d]", startYear, startQuarter, endYear, endQuarter, score);
		}
	}
	
	private final List<Entry> scoreData = new ArrayList<>();
	private final Map<Integer,String> quarter2BeginDate = new HashMap<>();
	private final Map<Integer,String> quarter2EndDate = new HashMap<>();
	
	private int minYear = 0;
	private int maxYear = 0;
	
	public CharlsonScore() {
		quarter2BeginDate.put(1, "0101");
		quarter2BeginDate.put(2, "0401");
		quarter2BeginDate.put(3, "0701");
		quarter2BeginDate.put(4, "1001");
		
		quarter2EndDate.put(1, "0331");
		quarter2EndDate.put(2, "0630");
		quarter2EndDate.put(3, "0930");
		quarter2EndDate.put(4, "1231");
	}
	
	/**
	 * Updates an old weight or adds a new entry
	 * @param quarter
	 * @param year
	 * @param diseaseClass
	 * @param weight
	 */
	public void updateWeightOrAddEntry(int quarter, int year, String diseaseClass, int weight) {
		//search an existing entry for the disease class
		for(Entry e : scoreData) {
			if(e.getQuarter() == quarter && e.getYear() == year && e.getDiseaseClass().equalsIgnoreCase(diseaseClass)) {
				e.updateWeight(weight);
				return;
			}
		}
		
		//no matching entry was found
		scoreData.add(new Entry(year, quarter, diseaseClass, weight));
	}
	
	/**
	 * Calculates the charlson score for four consecutive quarters
	 * 
	 * @return one line per entry 
	 */
	public Collection<String> calculateSlidingScore() {
		int lowerIndex = 0;
		int upperIndex = 0;
		Collection<SlidingWindow> scores = new ArrayList<>(100);
		while(lowerIndex < scoreData.size()) {
			Entry left = scoreData.get(lowerIndex);
			if(endQuarterLiesInFuture(left)) {
				lowerIndex++;
				continue;
			}
			upperIndex = findNextEndQuarterIndex(upperIndex, left);

			List<Entry> quarters = scoreData.subList(lowerIndex, upperIndex);
			scores.add(calculateScore(quarters));
			
			//move on to the next quarter
			lowerIndex = findNextStartQuarterIndex(lowerIndex, left);	
		}

		Collection<SlidingWindow> paddedScores = fillMissingQuartersWithZeros(scores);
		return createScoreRows(paddedScores);
	}

	private boolean endQuarterLiesInFuture(Entry left) {
		return left.getQuarter() > 1 && (left.getYear() + 1) > maxYear;
	}

	private Collection<String> createScoreRows(Collection<SlidingWindow> paddedScores) {
		return paddedScores
			.stream()
			.map(this::createOutput)
			.collect(Collectors.toList());
	}
	
	private String createOutput(SlidingWindow w) {
		return String.format("%d%s;%d%s;%d", w.getStartYear(), quarter2BeginDate.get(w.getStartQuarter()), w.getEndYear(), quarter2EndDate.get(w.getEndQuarter()), w.getScore());
	}
	
	private Collection<SlidingWindow> fillMissingQuartersWithZeros(Collection<SlidingWindow> scores) {
		Collection<SlidingWindow> interpolated = new ArrayList<>(scores.size() * 2);
		int expectedQuarter = 1;
		int expectedYear = minYear;
		for(SlidingWindow window : scores) {
			if(window.getStartQuarter() == expectedQuarter && window.getStartYear() == expectedYear) {
				interpolated.add(window);
				//assume that all windows are present and follow in order
				expectedQuarter = window.getStartQuarter() + 1;
				expectedYear = window.getStartYear();
				//handle overflow if Q4 of year was seen
				if(expectedQuarter > 4) {
					expectedQuarter = 1;
					expectedYear++;
				}
				continue;
			}
			interpolated.addAll(generateWindowPadding(window, expectedQuarter, expectedYear));
			interpolated.add(window);
		}

		return interpolated;
	}
	
	/**
	 * generates zero entries left to an entry
	 * @param window
	 * @param startQuarter
	 * @param startYear
	 * @return
	 */
	private Collection<SlidingWindow> generateWindowPadding(SlidingWindow window, int startQuarter, int startYear) {
		int numberOfPaddings = calculatePaddingCount(window, startQuarter, startYear);
		Collection<SlidingWindow> paddings = new ArrayList<>(numberOfPaddings);
		for(int paddingIndex = 0; paddingIndex < numberOfPaddings; paddingIndex++) {
			//padding starts at given quarter with offset
			int paddingStartQuarter = startQuarter + paddingIndex;
			int paddingStartYear = startYear;
			//handle crossing of year boundary
			if(paddingStartQuarter > 4) {
				paddingStartQuarter = 1;
				paddingStartYear++;
			}
			//for start quarter 1 end quarter is 4
			//otherwise end quarter equals the current start quarter plus three additional (total count of quarters is 4) modulo 4 available quarters
			int paddingEndQuarter = paddingStartQuarter == 1 ? 4 : (paddingStartQuarter + 3) % 4;
			//assume no year change
			int paddingEndYear = paddingStartYear;
			//if year boundary was crossed
			//end year is subsequent year
			if(paddingEndQuarter != 4)
				paddingEndYear = paddingStartYear + 1;
			//do not add padding into future
			if(paddingEndYear > maxYear)
				continue;

			//padding entries must obtain value of current window if it is in temporal proximity to window
			int weight = 0;
			if(isWindowEnclosedInPaddingEntry(window, paddingStartQuarter, paddingStartYear, paddingEndQuarter, paddingEndYear))
				weight = window.getScore();
			
			paddings.add(new SlidingWindow(paddingStartYear, paddingStartQuarter, paddingEndYear, paddingEndQuarter, weight));
		}
		return paddings;
	}

	private boolean isWindowEnclosedInPaddingEntry(SlidingWindow window, int paddingStartQuarter, int paddingStartYear,	int paddingEndQuarter, int paddingEndYear) {
		if(paddingStartQuarter == 1)
			return window.getStartYear() == paddingStartYear && window.getStartQuarter() >= paddingStartQuarter && window.getStartQuarter() <= paddingEndQuarter;
		
		return (window.getStartYear() <= paddingEndYear && window.getStartQuarter() <= paddingEndQuarter) ||
				(window.getStartYear() == paddingStartYear && window.getStartQuarter() >= paddingStartQuarter);
	}

	private int calculatePaddingCount(SlidingWindow w, int startQuarter, int startYear) {
		if(w.getStartYear() == startYear)
			return w.getStartQuarter() - startQuarter;
		//padding equals sum of quarters in year difference and quarters not constituting a full year 
		if(w.getStartYear() > startYear)
			return (w.getStartYear() - startYear) * 4 + w.getStartQuarter() - startQuarter;
		return 0;
	}
	
	private int findNextEndQuarterIndex(int upperIndex, Entry left) {
		//return the size of the data collection as everything remaining should be present in the sublist
		if (upperIndex == scoreData.size()) return upperIndex;
				
		//stop upperIndex search if right quarter is four quarters away
		//and if upperIndex reached the last position
		int i = upperIndex;
		while(i < scoreData.size() && !isFourQuartersAway(left, scoreData.get(i))) {
			i++;
		}
		
		return i;
	}
	
	private SlidingWindow calculateScore(List<Entry> quarters) {
		Entry qStart = quarters.get(0);
		Entry qEnd = quarters.get(quarters.size() - 1);
		Map<String,Integer> class2weight = new HashMap<>();

		//set or update the charlson scores
		for(Entry e : quarters) {
			if(class2weight.containsKey(e.getDiseaseClass())) {
				int oldWeight = class2weight.get(e.getDiseaseClass());
				class2weight.put(e.getDiseaseClass(), Math.max(oldWeight, e.getWeight()));
			} else
			class2weight.put(e.getDiseaseClass(), e.getWeight());		
		}
		
		//sum scores across disease classes
		int score = 0;
		for(int value : class2weight.values())
			score += value;

		//assume that sliding window has started in the first quarter of a year
		//thus the frame ends at the fourth quarter of the same year
		int endYear = qStart.getYear();
		int endQuarter = 4;
		//in case start quarter was not the first quarter
		//take year transition into account
		if(qStart.getQuarter() != 1) {
			endYear = qEnd.getYear();
			endQuarter = qStart.getQuarter() - 1;
		}
		return new SlidingWindow(qStart.getYear(), qStart.getQuarter(), endYear, endQuarter, score);
	}
	
	/**
	 * Determines whether another entry is four quarters away. This is achieved by comparing the year distance and the quarter index.
	 * Example:
	 * Q1 Q2 Q3 Q4           Q1 Q2 Q3 Q4
	 *    2010                   2011
	 * If the current quarter is Q1 2010 then the span of four quarters ends at Q1 2011. Therefore, Q1-4 2010 are included. The year difference equals one and the quarter index is also identical.
	 *    
	 * @param left the sooner year-quarter entry
	 * @param right the later year-quarter entry
	 * @return true if the distance spans four quarters
	 */
	public boolean isFourQuartersAway(Entry left, Entry right) {
		int delta = Math.abs(right.getYear() - left.getYear());
		
		//if year difference exceeds 1 the current date is more than 4 quarters away
		//if year distance equals 1 look for the next quarter in the following year that is larger or equal the current year
		//that terminates the four quarter span
		return delta > 1 || delta == 1 && right.getQuarter() >= left.getQuarter();
	}
	
	private int findNextStartQuarterIndex(int start, Entry yearQuarter) {
		int i = start + 1;
		if(i == scoreData.size()) return i;
		
		Entry current = scoreData.get(i);
		
		while(i < scoreData.size() && current.getYear() == yearQuarter.getYear() && current.getQuarter() == yearQuarter.getQuarter()) {
			if(i + 1 == scoreData.size()) break;
			current = scoreData.get(++i);
		}
				
		return i;
	}

	/**
	 * Set the year limits
	 * @param inclusiveMin
	 * @param inclusiveMax
	 */
	public void setYearLimits(int inclusiveMin, int inclusiveMax) {
		minYear = inclusiveMin;
		maxYear = inclusiveMax;
	}
}
