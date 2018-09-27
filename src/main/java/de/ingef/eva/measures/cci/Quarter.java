package de.ingef.eva.measures.cci;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

@Getter
@AllArgsConstructor
@EqualsAndHashCode
@ToString
public class Quarter {
	private int year;
	private int quarter;
			
	/**
	 * @return new quarter shifted by one quarter
	 */
	public Quarter increment() {
		int newQuarter = quarter + 1;
		int newYear = year;
		if(newQuarter > 4) {
			newQuarter = 1;
			newYear++;
		}
		
		return new Quarter(newYear, newQuarter);
	}
	
	public static int distanceInQuarters(Quarter a, Quarter b) {
		if(a.getYear() == b.getYear())
			return Math.abs(a.getQuarter() - b.getQuarter());

		Quarter lower = a;
		Quarter upper = b;
		if(a.getYear() > b.getYear()) {
			lower = b;
			upper = a;
		}
		int distance = 0;
		Quarter running = new Quarter(lower.getYear(), lower.getQuarter());
		while(running.getYear() != upper.getYear() || running.getQuarter() != upper.getQuarter())  {
			distance++;
			running = running.increment();
		}
		
		return distance;
	}
}