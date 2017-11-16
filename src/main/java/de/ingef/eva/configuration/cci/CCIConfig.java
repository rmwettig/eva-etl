package de.ingef.eva.configuration.cci;

import java.nio.file.Path;
import java.util.List;

import lombok.Getter;

@Getter
public class CCIConfig {
	/**
	 * csv file that contains icd codes, disease class and score (in this order)
	 */
	private Path cciFile;
	private int startYear;
	private int endYear;
	private List<CCISource> sources;
	private String tempDb;
}
