package de.ingef.eva.etl.filters;

import java.util.Map;

import de.ingef.eva.configuration.Configuration;
import de.ingef.eva.etl.Row;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * Filter base class
 */
@Getter @Setter
@NoArgsConstructor
public class Filter {

	/**
	 * Filter name
	 */
	protected String name;

	/**
	 * Database on which this filter should be applied
	 */
	private String database;

	/**
	 * Table on which this filter should be applied
	 */
	private String table;

	/**
	 * Column which is filtered
	 */
	private String column;

	/**
	 * Filter behaviour
	 */
	private FilterStrategy filterStrategy;
	/**
	 * Determines if values validated by the filter strategy are accepted and passed on in the pipeline
	 */
	private FilterMode mode;
	
	public static final Filter NOP_FILTER = new NOPFilter();
	
	public enum FilterMode {
		INCLUDE,
		EXCLUDE
	}
	
	private static class NOPFilter extends Filter {

		public NOPFilter() {
			name = "NOPFilter";
		}

		@Override
		public boolean isValid(Row row) {
			return true;
		}
		
	}
	
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
		boolean isValid = filterStrategy.isValid(row.getColumns().get(columnIndex).getContent());
		return mode == FilterMode.INCLUDE ? isValid : !isValid;
	}
	
	public void initialize(Configuration config) {
		filterStrategy.initialize(config);
	}
}
