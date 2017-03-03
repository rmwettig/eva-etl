package de.ingef.eva.utility;

import java.io.File;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.logging.log4j.Logger;

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
	
	/**
	 * Calculates years that lie between start and and year
	 * @param queryConfig
	 * @return array of years including start and end years
	 */
	public static int[] extractYears(DatabaseQueryConfiguration queryConfig)
	{
		return extractYears(queryConfig.getStartYear(), queryConfig.getEndYear());
	}
	
	public static int[] extractYears(int start, int end)
	{
		//include start and end year
		int delta = end - start + 1;
		int[] years = new int[delta];
		for(int i = 0; i < delta; i++)
		{
			years[i] = start + i;
		}
		return years;
	}
	
	/**
	 * Logs error messages for all nested sql exceptions
	 * @param logger logger instance
	 * @param root first sql exception retrieved
	 * @param message template string for the message
	 */
	public static void logSqlExceptions(Logger logger, SQLException root, String message)
	{
		logger.error(message, root.getMessage(), root.getStackTrace());
		SQLException child = root.getNextException();
		while(child != null)
		{
			logger.error(message, child.getMessage(), child.getStackTrace());
			child = child.getNextException();
		}
	}
	
	public static List<Dataset> findDatasets(File[] files)
	{
		Set<String> done = new HashSet<String>();
		List<Dataset> datasets = new ArrayList<Dataset>(20);
		for(File file : files)
		{
			//database dump files are prefixed with 
			//database name and table e.g. db_table.x.csv
			String commonName = file.getName();
			commonName = commonName.substring(0, commonName.indexOf("."));
			
			//if there is an entry for the prefix all files are found already
			if(!done.contains(commonName))
			{
				Dataset ds = new Dataset(commonName);
				
				for(File f : files)
				{
					String fname = f.getName();
					if(fname.startsWith(commonName))
					{
						if(!fname.contains("header"))
							ds.addFile(f);
						else
							ds.setHeaderFile(f);
					}
				}
				datasets.add(ds);
				done.add(commonName);
			}
		}
		
		return datasets;
	}
	
	public static StringBuilder mergeStrings(Collection<String> elements, String delimiter)
	{
		StringBuilder merged = new StringBuilder();
		Iterator<String> iter = elements.iterator();
		while(iter.hasNext())
		{
			merged.append(iter.next());
			if(iter.hasNext())
				merged.append(delimiter);
		}
		return merged;
	}
}
