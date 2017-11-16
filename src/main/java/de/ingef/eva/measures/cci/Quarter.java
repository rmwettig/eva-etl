package de.ingef.eva.measures.cci;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor 
public class Quarter {
	private int year;
	private int quarter;
			
	/**
	 * shifts by one quarter
	 */
	public void increment() {
		++quarter;
		if(quarter > 4) {
			quarter = 1;
			year++;
		}
	}
}