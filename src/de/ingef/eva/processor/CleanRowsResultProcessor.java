package de.ingef.eva.processor;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;

/***
 * This processor removes control sequences, e.g. tab or newline, and whitespaces at the beginning and end of an entry.
 * @author Martin Wettig
 *
 */
public class CleanRowsResultProcessor implements ResultProcessor {

	public Collection<String> ProcessResults(ResultSet results) {
		ArrayList<String> processedRows = new ArrayList<String>(1000);
		
		try {
			int columnCount = results.getMetaData().getColumnCount();
			StringBuilder cleanRow = new StringBuilder();
			while(results.next())
			{
				for(int i = 1; i <= columnCount; i++)
				{
					cleanRow.append(removeBoundaryWhitespaces(removeControlSequences(results.getString(i))));
					// do not append a delimiter to the last entry in a row
					if(i != columnCount) cleanRow.append(';');
				}
				processedRows.add(cleanRow.toString());				
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		return processedRows;
	}
	
	private String removeControlSequences(String row)
	{
		return row.replace("\r", "").replace("\n", "").replace("\t", "");
	}
	
	private String removeBoundaryWhitespaces(String row)
	{
		return row.trim();
	}

}
