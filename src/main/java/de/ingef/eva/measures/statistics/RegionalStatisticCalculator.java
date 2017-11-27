package de.ingef.eva.measures.statistics;

import java.util.Arrays;
import java.util.List;

import de.ingef.eva.measures.cci.Quarter;
import de.ingef.eva.utility.QuarterCount;

public class RegionalStatisticCalculator {

	/**
	 * 
	 * 
	 * @param referenceQuarter latest quarter year pair found in table of interest
	 * @param absoluteNumbers data points for current and previous year
	 * 		Content example:
	 * 		year quarter kv    kv_name		  	  count			
	 * 		2016	  4     01	 Schleswig-Holstein	  1256
	 * 		2015	  4	    01	 Schleswig-Holstein   1287
	 * @param kv two digit id, e.g. "01"
	 * @param kvName label of the kv id, e.g. "Schleswig-Holstein"
	 * @return
	 */
	public StatisticsEntry calculateRegionalStatistic(Quarter referenceQuarter, List<QuarterCount> absoluteNumbers, String kv, String kvName) {
		if(absoluteNumbers.size() < 2)
			return createZeroChangeEntry(referenceQuarter, absoluteNumbers.get(0), kv, kvName);
		return calculateChangeRatio(referenceQuarter, absoluteNumbers, kv, kvName);
	}
	
	private StatisticsEntry createZeroChangeEntry(Quarter referenceQuarter, QuarterCount quarterCount, String kv, String kvName) {
		Quarter quarter = new Quarter(referenceQuarter.getYear(), referenceQuarter.getQuarter());
		int count = quarterCount.getQuarter().equals(referenceQuarter) ? quarterCount.getCount() : 0; 
		return new StatisticsEntry(kvName, kv, Arrays.asList(new QuarterCount(quarter, count)));
	}
	
	private StatisticsEntry calculateChangeRatio(Quarter referenceQuarter, List<QuarterCount> absoluteNumbers, String kv, String kvName) {
		QuarterCount reference =  absoluteNumbers.get(0);
		QuarterCount comparedQuarter = absoluteNumbers.get(1);
		//expected reference numbers are  not from the intended reference quarter
		if(!reference.getQuarter().equals(referenceQuarter)) {
			//reference is also not found at the position of the expected comparison quarter (handling switched positions)
			if(!comparedQuarter.getQuarter().equals(referenceQuarter)) {
				return createZeroChangeEntry(referenceQuarter, new QuarterCount(referenceQuarter, 0), kv, kvName);
			} else {
				QuarterCount temp = comparedQuarter;
				comparedQuarter = reference;
				reference = temp;
			}
		}
		//compared quarter is not the quarter one year back
		if(Quarter.distanceInQuarters(comparedQuarter.getQuarter(), referenceQuarter) != 4) {
			return createZeroChangeEntry(referenceQuarter, reference, kv, kvName);
		}
		
		QuarterCount qc = new QuarterCount(reference.getQuarter(), reference.getCount());
		double ratio = comparedQuarter.getCount() > 0 ? reference.getCount() / (double) comparedQuarter.getCount() : 0;
		qc.setChangeRatio(ratio);
		return new StatisticsEntry(kvName, kv, Arrays.asList(qc));
	}
}