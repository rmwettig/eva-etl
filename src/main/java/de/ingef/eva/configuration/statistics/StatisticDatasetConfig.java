package de.ingef.eva.configuration.statistics;

import java.util.List;

import de.ingef.eva.measures.statistics.DataSlice;
import lombok.Getter;

/**
 * Report creation definition
 */
@Getter
public class StatisticDatasetConfig {
	/**
	 * ADB or FDB
	 */
	private String db;
	private String dataset;
	private List<DataSlice> views;
	/**
	 * only used for ADB
	 */
	private List<String> h2iks;
	/**
	 * only used for FDB
	 */
	private String flag;
	private int startYear;
	private int endYear;
}
