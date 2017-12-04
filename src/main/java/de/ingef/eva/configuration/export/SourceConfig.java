package de.ingef.eva.configuration.export;

import java.util.List;

import lombok.Getter;
import lombok.ToString;

@Getter
@ToString
public class SourceConfig {
	private String db;
	private String datasetName;
	private List<ViewConfig> views;
	private List<WhereConfig> where;
}
