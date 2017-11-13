package de.ingef.eva.configuration.export;

import java.util.List;

import lombok.Getter;

@Getter
public class ExportConfig {
	private int startYear;
	private int endYear;
	private int numberOfPreviousYears;
	private List<SourceConfig> sources;
}
