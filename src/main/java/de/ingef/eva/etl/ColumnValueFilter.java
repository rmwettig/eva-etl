package de.ingef.eva.etl;

import java.util.Map;
import java.util.regex.Pattern;

/**
 * Checks if a row column has values matching the given pattern
 * @author Martin.Wettig
 *
 */
public class ColumnValueFilter extends Filter {

	private String database;
	private String table;
	private String column;
	private Pattern regex;
	
	public ColumnValueFilter(String name, String columnName, String regexPattern, String databaseName, String tableName) {
		super(name);
		regex = Pattern.compile(regexPattern);
		column = columnName.toLowerCase();
		database = databaseName;
		table = tableName;
	}
	
	@Override
	public boolean isValid(Row row) {
		Map<String,Integer> columnIndices = row.getColumnName2Index();
		//do not process row
		//  if column is not present
		//  if database is not set, empty or does not match
		//  if table is not set, empty or does not match
		//  both database and table are optional fields
		if(!columnIndices.containsKey(column) &&
			(database == null || database.isEmpty() || !database.equalsIgnoreCase(row.getDb())) &&
			(table == null || table.isEmpty() || !table.equalsIgnoreCase(row.getTable()))
		)
			return true;
		
		int columnIndex = columnIndices.get(column);
		String value = row.getColumns().get(columnIndex).getContent();
		return regex.matcher(value).matches();
	}

}
