package de.ingef.eva.utility;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;

import de.ingef.eva.configuration.DatabaseQueryConfiguration;

public class Helper 
{
	public static long NanoSecondsToMinutes(long value)
	{
		return value/60000000000L;
	}
	
	
	public static Collection<String[]> convertResultSet(ResultSet results) throws SQLException
	{
		ArrayList<String[]> converted = new ArrayList<String[]>(1000);

		String[] names = extractColumnNames(results);
		if(names != null)
			converted.add(names);
		int columnCount = results.getMetaData().getColumnCount();
		while(results.next())
		{
			String[] row = new String[columnCount];
			for(int i = 0; i < columnCount; i++)
			{
				//index of sql set starts at 1
				String content = results.getString(i+1); 
				row[i] = content != null ? content : "";
			}
			converted.add(row);
		}
			
		return converted;
	}
	
	public static String[] extractColumnNames(ResultSet results) throws SQLException
	{
		String[] names = null;
		ResultSetMetaData metadata = results.getMetaData();
		int columnCount = metadata.getColumnCount();
		names = new String[columnCount];
		for(int i = 0; i < columnCount; i++)
			names[i] = metadata.getColumnName(i+1);

		return names;
	}
	
	public static int[] extractYears(DatabaseQueryConfiguration queryConfig)
	{
		//include start and end year
		int delta = queryConfig.getEndYear() - queryConfig.getStartYear() + 2;
		int[] years = new int[delta];
		for(int i = 0; i < delta; i++)
		{
			years[i] = queryConfig.getStartYear() + i;
		}
		return years;
	}
}
