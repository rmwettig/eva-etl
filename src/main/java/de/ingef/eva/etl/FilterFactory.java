package de.ingef.eva.etl;

import java.util.List;
import java.util.stream.Collectors;

public class FilterFactory {

	public List<Filter> create(List<FilterConfig> filterConfiguration) {
		return filterConfiguration.stream().map(this::of).collect(Collectors.toList());
	}
	
	private Filter of(FilterConfig config) {
		String dbName = config.getDatabaseName() == null ? "" : config.getDatabaseName();
		String tableName = config.getTableName() == null ? "" : config.getTableName();
		return new ColumnValueFilter(config.getFilterName(), config.getColumnName(), config.getRegex(), dbName, tableName);
	}
}
