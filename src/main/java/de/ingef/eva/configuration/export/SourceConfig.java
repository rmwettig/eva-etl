package de.ingef.eva.configuration.export;

import java.util.List;

import lombok.Getter;

@Getter
public class SourceConfig {
	private String db;
	private String datasetName;
	private List<ViewConfig> views;
	private List<WhereConfig> where;
}
