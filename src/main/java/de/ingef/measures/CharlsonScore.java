package de.ingef.measures;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * Aggregates icd class weights for a pid.
 * It assumes that entries are added in order, i.e. data must begin with smallest year Q1 and end with largest year Q4
 * 
 * @author Martin Wettig
 *
 */
public class CharlsonScore {
	
	@AllArgsConstructor
	private final class Entry {
		@Getter
		private final int year;
		@Getter
		private final int quarter;
		@Getter
		private final String diseaseClass;
		@Getter
		private int weight = 0;
		
		public void updateWeight(int newWeight) {
			weight = Math.max(weight, newWeight);
		}
	}
	
	private final List<Entry> scoreData = new ArrayList<Entry>();
	
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
	 * Calculates the morbiscore for four consecutive quarters
	 * 
	 * @return one line per entry 
	 */
	public Collection<String> calculateSlidingScore() {
		int lowerIndex = 0;
		int upperIndex = 0;
		Collection<String> scores = new ArrayList<String>(100);
		while(lowerIndex < scoreData.size()) {
			Entry left = scoreData.get(lowerIndex);
						
			upperIndex = updateUpperIndex(upperIndex, left);

			List<Entry> quarters = scoreData.subList(lowerIndex, upperIndex);
			scores.add(calculateScore(quarters));
			
			//move on to the next quarter
			lowerIndex = findNextQuarterIndex(lowerIndex, left);	
		}
		
		return scores;
	}

	private int updateUpperIndex(int upperIndex, Entry left) {
		//return the size of the data collection as everything remaining should be present in the sublist
		if (upperIndex == scoreData.size()) return upperIndex;
		
		//condition to include at most four quarters only:
		//(y0 == y1 && q0 <= q1) //this is valid for all entries when starting at yearQ1 up to yearQ4
		//|| (y0 != y1 && q0 > q1) //this is valid for all entries starting at yearQ2 up to year+1Q1
		
		//expected end year and quarter for index search
		//look three quarters ahead
		int q1 = left.getQuarter() + 3;
		int y1 = left.getYear();
		//if end quarter exceeds fourth quarter of a year
		if(q1 > 4) {
			//calculate quarters in the next year
			q1 = q1 - 4;
			//take year change into account
			y1 += 1;
		}
		
		//stop upperIndex search if right quarter is four quarters away
		//and if upperIndex reached the last position
		int i = upperIndex;
		while(i < scoreData.size() && !isFourQuartersAway(left, scoreData.get(i))) {
			i++;
		}
		
		return i;
	}
	
	private String calculateScore(List<Entry> quarters) {
		Entry qStart = quarters.get(0);
		Entry qEnd = quarters.get(quarters.size() - 1);
		Map<String,Integer> class2weight = new HashMap<String,Integer>();

		//set or update the morbiscores
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
		
		return String.format("%dQ%d;%dQ%d;%d", qStart.getYear(), qStart.getQuarter(), qEnd.getYear(), qEnd.getQuarter(), score);
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
	
	private int findNextQuarterIndex(int start, Entry yearQuarter) {
		int i = start + 1;
		if(i == scoreData.size()) return i;
		
		Entry current = scoreData.get(i);
		
		while(i < scoreData.size() &&
				current.getYear() == yearQuarter.getYear() && current.getQuarter() == yearQuarter.getQuarter()) {
			if(i + 1 == scoreData.size()) break;
			current = scoreData.get(++i);
		}
				
		return i;
	}
}
