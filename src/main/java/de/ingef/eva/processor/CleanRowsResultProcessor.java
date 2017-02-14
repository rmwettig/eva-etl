package de.ingef.eva.processor;

import java.util.ArrayList;
import java.util.Collection;

/***
 * This processor removes control sequences, e.g. tab or newline, and whitespaces at the beginning and end of an entry.
 * @author Martin Wettig
 *
 */
public class CleanRowsResultProcessor implements ResultProcessor {

	public Collection<String[]> ProcessResults(Collection<String[]> results) {
		ArrayList<String[]> processedRows = new ArrayList<String[]>(1000);
					
		//each result is a row
		for(String[] result : results)
		{
			String[] cleanRow = new String[result.length];
			int i = 0;
			for(String s : result)
			{					
				cleanRow[i++] = removeBoundaryWhitespaces(removeControlSequences(s));
			}
			processedRows.add(cleanRow);
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
