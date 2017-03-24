package de.ingef.eva.configuration;

import lombok.Getter;

/**
 * Data container that holds file name of a header and associated data
 * 
 * @author Martin Wettig
 *
 */
public class Target {
	@Getter
	private String headerFile;
	
	@Getter
	private String dataFile;
	
	public Target(String headerFile, String dataFile) {
		this.headerFile = headerFile;
		this.dataFile = dataFile;
	}
}
