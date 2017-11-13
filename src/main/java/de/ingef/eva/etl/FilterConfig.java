package de.ingef.eva.etl;

import lombok.Getter;

@Getter
public class FilterConfig {
	private String regex;
	private String columnName;
	private String filterName;
	private String description;
	private String tableName;
	private String databaseName;
}
