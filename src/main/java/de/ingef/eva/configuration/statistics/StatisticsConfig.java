package de.ingef.eva.configuration.statistics;

import java.util.List;

import de.ingef.eva.measures.statistics.DataSlice;
import lombok.Getter;

@Getter
public class StatisticsConfig {
	private String db;
	private String dataset;
	private List<DataSlice> views;
	private List<String> h2iks;
	private String flag;
	private int startYear;
	private int endYear;
}
