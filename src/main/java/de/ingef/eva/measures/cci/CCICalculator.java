package de.ingef.eva.measures.cci;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import lombok.Getter;

/**
 * Aggregates icd class weights for a pid.
 * It assumes that entries are added in order, i.e. data must begin with smallest year Q1 and end with largest year Q4
 * 
 * @author Martin Wettig
 *
 */
public class CCICalculator {
	
	private final int currentYear = Calendar.getInstance().get(Calendar.YEAR);
	
	/**
	 * Aggregates per class scores per quarter
	 * @author Martin.Wettig
	 *
	 */
	@Getter
	private static class QuarterScore {
		private Map<String,Integer> diseaseClass2Weight = new HashMap<>();
		
		/**
		 * keeps the maximum weight for a given class
		 * @param diseaseClass
		 * @param weight
		 */
		public void addClass(String diseaseClass, int weight) {
			if(diseaseClass2Weight.containsKey(diseaseClass)) {
				int newWeight = Math.max(weight, diseaseClass2Weight.get(diseaseClass));
				diseaseClass2Weight.put(diseaseClass, newWeight);
			} else {
				diseaseClass2Weight.put(diseaseClass, weight);
			}
		}
	}

	public QuarterScoreResult calculateSlidingWindow(List<QuarterEntry> data, int startYear, int startQuarter) {
		Quarter q = new Quarter(startYear, startQuarter);
		List<QuarterScore> scores = new ArrayList<>(4);
		for(int quarterIndex = 0; quarterIndex < 4; quarterIndex++) {
			scores.add(calculateQuarterScore(data, q.getYear(), q.getQuarter()));
			//do not increase quarter after the last quarter was processed
			if(quarterIndex < 3) q.increment();
			//abort calculation if next quarter is in future
			if(q.getYear() > currentYear) break;
		}
		
		return new QuarterScoreResult(new Quarter(startYear, startQuarter), clampQuarterToPresent(q), calculateFinalScore(scores));
	}

	private Quarter clampQuarterToPresent(Quarter q) {
		if(q.getYear() <= currentYear)
			return q;
		return new Quarter(currentYear, 4);
	}

	private QuarterScore calculateQuarterScore(List<QuarterEntry> windowData, int year, int quarter) {
		List<QuarterEntry> matchingLines = windowData
				.stream()
				.filter(e -> e.getYear() == year && e.getQuarter() == quarter)
				.collect(Collectors.toList());
		if(matchingLines.isEmpty())
			return new QuarterScore();
		QuarterScore qs = new QuarterScore();
		matchingLines
			.stream()
			.forEach(e -> qs.addClass(e.getDiseaseClass(), e.getWeight()));
		
		return qs;
	}

	private Map<String, Integer> calculateMaximumWeightPerDiseaseClass(List<QuarterScore> quarterScores) {
		Map<String,Integer> maxWeightOfSameClass = new HashMap<>();
		quarterScores
			.stream()
			.forEach(qs -> {
				qs.getDiseaseClass2Weight().forEach((diseaseClass,weight) -> {
					if(maxWeightOfSameClass.containsKey(diseaseClass))
						maxWeightOfSameClass.put(diseaseClass, Math.max(maxWeightOfSameClass.get(diseaseClass), weight));
					else
						maxWeightOfSameClass.put(diseaseClass, weight);
				});
			});
		return maxWeightOfSameClass;
	}
		
	private int calculateFinalScore(List<QuarterScore> quarterScores) {
		return calculateMaximumWeightPerDiseaseClass(quarterScores)
				.entrySet()
				.stream()
				.mapToInt(Map.Entry::getValue)
				.sum();
	}
}
