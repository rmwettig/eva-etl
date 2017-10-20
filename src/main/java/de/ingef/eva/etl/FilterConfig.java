package de.ingef.eva.etl;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

@Getter
@RequiredArgsConstructor
public class FilterConfig {
	private final String regex;
	private final String columnName;
	private final String filterName;
	private final String description;
	private final String tableName;
	private final String databaseName;
}
