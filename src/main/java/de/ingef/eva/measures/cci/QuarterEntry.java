package de.ingef.eva.measures.cci;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

/**
 * Data container for data returned from database
 * @author Martin.Wettig
 *
 */
@Getter
@RequiredArgsConstructor
public class QuarterEntry {
	private final int quarter;
	private final int year;
	private final String icdcode;
	private final String diseaseClass;
	private final int weight;
	private final String h2ik;
}