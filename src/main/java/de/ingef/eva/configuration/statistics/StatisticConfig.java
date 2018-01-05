package de.ingef.eva.configuration.statistics;

import java.nio.file.Path;
import java.util.List;

import lombok.Getter;

@Getter
public class StatisticConfig {
	/**
	 * path to the pre-calculated morbi statistic. Values must be separated by semicolons. The specified file can contain the information for multiple datasets.
	 * Expected column order: Kasse, Grouperjahr, Leistungsjahr, Berichtsjahr, Quartal, Konfiguration
	 */	
	private Path morbiStatisticFile;
	private List<StatisticDatasetConfig> datasets;
}
