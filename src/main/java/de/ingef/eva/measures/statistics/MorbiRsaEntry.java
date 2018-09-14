package de.ingef.eva.measures.statistics;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

@Getter
@RequiredArgsConstructor
public class MorbiRsaEntry {
	private final String insurance;
	private final String setup;
	private final int grouperYear;
	private final int benefitYear;
	private final int reportingYear;
	private final String quarter;
	private final String configuration;
}
