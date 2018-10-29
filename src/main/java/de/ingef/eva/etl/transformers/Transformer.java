package de.ingef.eva.etl.transformers;

import de.ingef.eva.etl.Row;
import lombok.RequiredArgsConstructor;

/**
 * Base class for transformers
 */
@RequiredArgsConstructor
public abstract class Transformer {
	
	private final String db;
	private final String table;
	
	public static class NOPTransformer extends Transformer {
		
		public NOPTransformer() {
			super("", "");
		}
		
		@Override
		public Row transform(Row row) {
			return row;
		}
		
	}
	
	public abstract Row transform(Row row);
	
	protected boolean canProcessRow(String rowDb, String rowTable) {
		boolean isTablePresent = table != null && !table.isEmpty();
		boolean isDbPresent = db != null && !db.isEmpty();
		
		if(isTablePresent && isDbPresent)
			return rowDb.toLowerCase().contains(db.toLowerCase()) && rowTable.toLowerCase().contains(table.toLowerCase());
		if(isTablePresent && !isDbPresent)
			return rowTable.toLowerCase().contains(table.toLowerCase());
		if(!isTablePresent && isDbPresent)
			return rowDb.toLowerCase().contains(db.toLowerCase());
		
		return false; 
	}
}
