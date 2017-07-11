package de.ingef.eva.dataprocessor;

import java.util.List;
import java.util.Map;

import de.ingef.eva.data.DataTable;
import de.ingef.eva.data.RowElement;
import de.ingef.eva.datasource.DataProcessor;
import de.ingef.eva.error.DataTableOperationException;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;

/**
 * Appends a column based on a given key. The expected format of the source file is 'key;value'
 * @author Martin.Wettig
 *
 */
@Log4j2
@RequiredArgsConstructor
public class ColumnAppender implements DataProcessor {

	private final String sourceKeyColumn;
	private final String targetKeyColumn;
	private final Map<String,String> key2column;
	
	/**
	 * adds columns to a target data table (the second parameter) read from the source data table (first parameter)
	 */
	@Override
	public DataTable process(DataTable... dataTables) {
		DataTable source = dataTables[0];
		DataTable target = dataTables[1];
		
		try {
			source.open();
			target.open();
			List<RowElement> header = target.getColumnNames();
			int targetColumnIndex = findColumnIndex(header, targetKeyColumn);
			
			//add new column's name
			header.add(source.getColumnNames().get(1));
			
		} catch(DataTableOperationException e) {
			
		}
		
		return null;
	}
	
	/**
	 * finds index of named column
	 * @param header
	 * @param columnName
	 * @return -1 if column does not exist
	 */
	private int findColumnIndex(List<RowElement> header, String columnName) {
		for(int i = 0; i < header.size(); i++) {
			if(header.get(i).getName().equalsIgnoreCase(columnName))
				return i;
		}
		
		return -1;
	}
}
