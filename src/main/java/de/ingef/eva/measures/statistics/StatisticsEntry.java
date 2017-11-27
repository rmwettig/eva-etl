package de.ingef.eva.measures.statistics;

import java.util.List;

import de.ingef.eva.utility.QuarterCount;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

@Getter
@RequiredArgsConstructor
@ToString
public class StatisticsEntry {
	private final String label;
	private final String identifier;
	private final List<QuarterCount> dataCount;
}
