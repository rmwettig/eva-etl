package de.ingef.eva.etl;

import java.util.Map;
import java.util.regex.Pattern;

/**
 * Checks if a row column has values matching the given pattern
 * @author Martin.Wettig
 *
 */
public class ColumnValueFilter extends Filter {

	private String column;
	private Pattern regex;
	
	public ColumnValueFilter(String name, String columnName, String regexPattern) {
		super(name);
		regex = Pattern.compile(regexPattern);
		column = columnName.toLowerCase();
	}
	
	@Override
	public boolean isValid(Row row) {
		Map<String,Integer> columnIndices = row.getColumnName2Index();
		//column is not present so assume that row is valid
		if(!columnIndices.containsKey(column))
			return true;
		int columnIndex = columnIndices.get(column);
		String value = row.getColumns().get(columnIndex).getContent();
		return regex.matcher(value).matches();
	}

}
