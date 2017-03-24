package de.ingef.eva.configuration;

import java.util.Collection;

import lombok.Getter;

public class Mapping {
		
	@Getter
	private String mappingFileName;
	
	@Getter
	private String sourceColumn;
	
	@Getter
	private String targetColumn;
	
	@Getter
	private Collection<Target> targets;
	
	public Mapping(String mappingFile, String sourceColumn, String targetColumn, Collection<Target> targets) {
		this.mappingFileName = mappingFile;
		this.sourceColumn = sourceColumn;
		this.targetColumn = targetColumn;
		this.targets = targets;
	}
}
