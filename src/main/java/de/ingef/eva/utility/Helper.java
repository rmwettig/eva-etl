package de.ingef.eva.utility;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.logging.log4j.Logger;


public class Helper {
	
	public static long NanoSecondsToMinutes(long value) {
		return value / 60000000000L;
	}

	public static Collection<String[]> convertResultSet(ResultSet results) throws SQLException {
		ArrayList<String[]> converted = new ArrayList<String[]>(1000);

		String[] names = extractColumnNames(results);
		if (names != null)
			converted.add(names);
		int columnCount = results.getMetaData().getColumnCount();
		while (results.next()) {
			String[] row = new String[columnCount];
			for (int i = 0; i < columnCount; i++) {
				// index of sql set starts at 1
				String content = results.getString(i + 1);
				row[i] = content != null ? content : "";
			}
			converted.add(row);
		}

		return converted;
	}

	public static String[] extractColumnNames(ResultSet results) throws SQLException {
		String[] names = null;
		ResultSetMetaData metadata = results.getMetaData();
		int columnCount = metadata.getColumnCount();
		names = new String[columnCount];
		for (int i = 0; i < columnCount; i++)
			names[i] = metadata.getColumnName(i + 1);

		return names;
	}
	
	/**
	 * Calculates years which lie between start and end
	 * @param start
	 * @param end
	 * @return
	 */
	public static int[] extractYears(int start, int end) {
		// include start and end year
		int delta = end - start + 1;
		int[] years = new int[delta];
		for (int i = 0; i < delta; i++) {
			years[i] = start + i;
		}
		
		return years;
	}

	/**
	 * Logs error messages for all nested sql exceptions
	 * 
	 * @param logger
	 *            logger instance
	 * @param root
	 *            first sql exception retrieved
	 * @param message
	 *            template string for the message
	 */
	public static void logSqlExceptions(Logger logger, SQLException root, String message) {
		logger.error(message, root.getMessage(), root.getStackTrace());
		SQLException child = root.getNextException();
		while (child != null) {
			logger.error(message, child.getMessage(), child.getStackTrace());
			child = child.getNextException();
		}
	}

	public static List<Dataset> findDatasets(File[] files) {
		Set<String> done = new HashSet<String>();
		List<Dataset> datasets = new ArrayList<Dataset>(20);
		for (File file : files) {
			if (file.isDirectory()) continue;
			// database dump files are prefixed with
			// database name and table e.g. db_table.x.csv
			String commonName = file.getName();
			commonName = commonName.substring(0, commonName.indexOf("."));

			// if there is an entry for the prefix all files are found already
			if (!done.contains(commonName)) {
				Dataset ds = new Dataset(commonName);

				for (File f : files) {
					String fname = f.getName();
					if (fname.startsWith(commonName)) {
						if (fname.endsWith("header.csv"))
							ds.setHeaderFile(f);
						else if(fname.endsWith("csv"))
							ds.addFile(f);
					}
				}
				datasets.add(ds);
				done.add(commonName);
			}
		}

		return datasets;
	}

	public static StringBuilder mergeStrings(Collection<String> elements, String delimiter) {
		StringBuilder merged = new StringBuilder();
		Iterator<String> iter = elements.iterator();
		while (iter.hasNext()) {
			merged.append(iter.next());
			if (iter.hasNext())
				merged.append(delimiter);
		}
		
		return merged;
	}

	public static Map<String, String> createMappingFromFile(String fileName) {
		final Map<String, String> mapping = new HashMap<String,String>();
		try (BufferedReader reader = new BufferedReader(new FileReader(fileName));) {
			String line;
			while((line = reader.readLine()) != null) {
				int separatorIndex = line.indexOf(";");
				mapping.put(line.substring(0, separatorIndex), line.substring(separatorIndex + 1));
			}
			
			return mapping;
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return mapping;
	}
	
	/**
	 * Finds the index of a named column by examining the header file
	 * @param fileName header
	 * @param delimiter column separator
	 * @param columnName searched column
	 * @return column index or -1 if column was not found
	 */
	public static int findColumnIndexfromHeaderFile(String fileName, String delimiter, String columnName) {
		try (BufferedReader reader = new BufferedReader(new FileReader(fileName));) {
			final String[] header = reader.readLine().split(delimiter);
			
			for(int i = 0; i < header.length; i++) {
				if(header[i].equalsIgnoreCase(columnName)) {
					return i;
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return -1;
	}
	
	public static void createFolders(String path) {
		File f = new File(path);
		if(!f.exists())
			f.mkdirs();
	}
}
