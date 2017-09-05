package de.ingef.eva.utility;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.stream.IntStream;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import de.ingef.eva.constant.OutputDirectory;
import de.ingef.eva.data.DataSet;
import de.ingef.eva.data.DataTable;
import de.ingef.eva.data.RowElement;
import de.ingef.eva.data.SimpleRowElement;
import de.ingef.eva.data.TeradataColumnType;
import de.ingef.eva.database.Column;
import de.ingef.eva.database.TextColumn;
import de.ingef.eva.datasource.file.FastExportFileDataTable;
import de.ingef.eva.datasource.file.FileDataTable;

public final class Helper {
	public static void createFolders(String path) {
		File f = new File(path);
		if(!f.exists())
			f.mkdirs();
	}
	
	public static float millisecondsToMinutes(long value) {
		return value / (float) 60000;
	}
	
	/**
	 * 
	 * @param directory directory which contains data files
	 * @param extension file extension that is expected
	 * @param headers table name to columns map
	 * @param rowPrefix prefix for each row indicating beginning of actual data
	 * @param infix a part that must occur in the filename
	 * @return
	 */
	public static Collection<DataTable> loadDataTablesFromDirectory(String directory, String extension, Map<String,List<Column>> headers, String rowPrefix, String delimiter, String infix) {		
		File rawDirectory = new File(directory);
		Collection<DataTable> tables = new ArrayList<>();
		if(!rawDirectory.exists()) return tables;
		for(File f : rawDirectory.listFiles()) {
			if(f.isDirectory()) continue;
			String fileName = f.getName();
			if(!fileName.endsWith(extension) && (infix.isEmpty() || !fileName.contains(infix))) continue;
			//expected filename has the form ACC_DB_TABLE.yyyy.csv
			String commonFileName = fileName.substring(0, fileName.indexOf("."));
			if(!headers.containsKey(commonFileName)) continue;
			List<Column> columns = headers.get(commonFileName);
			
			List<RowElement> header = new ArrayList<>(columns.size());
			for(int i = 0; i < columns.size(); i++) {
				Column c = columns.get(i);
				header.add(new SimpleRowElement(c.getName(), i, c.getType(), c.getName()));
			}
			DataTable dt;
			//FIXME add File loader that can be injected
			if(directory.toLowerCase().contains(OutputDirectory.RAW.toLowerCase()))
				dt = new FastExportFileDataTable(f, delimiter, fileName.substring(0, fileName.lastIndexOf(".")), header, rowPrefix);
			else if (directory.toLowerCase().contains(OutputDirectory.CLEAN.toLowerCase()))
				dt = new FileDataTable(f, delimiter, fileName.substring(0, fileName.lastIndexOf(".")), header);
			else
				dt = new FileDataTable(f, delimiter, commonFileName, header);
			
			tables.add(dt);
		}
		
		return tables;
	}
	
	public static DataTable loadExternalDataTable(Path path) throws IOException {
		try(BufferedReader reader = Files.newBufferedReader(path);) {
			String[] header = reader.readLine().split(";");
			reader.close();
			List<RowElement> columnHeader = new ArrayList<>(header.length);
			IntStream
				.range(0, header.length)
				.forEachOrdered(i -> columnHeader.add(new SimpleRowElement(header[i], i, TeradataColumnType.UNKNOWN, header[i])));
			
			File f = path.toFile();
			return new FileDataTable(f, ";", f.getName().substring(0, f.getName().indexOf(".")), columnHeader);
		}
	}
	
	public static Map<String, List<Column>> parseTableHeaders(String path) throws JsonProcessingException, IOException {
		File directory = new File(path);
		Map<String, List<Column>> table2Headers = new HashMap<>();
		for(File header : directory.listFiles()) {
			String fileName = header.getName();
			if(!fileName.endsWith(".header")) continue;
			JsonNode root = new ObjectMapper().readTree(header);
			JsonNode columnsNode = root.path("columns");
			List<Column> columns = new ArrayList<>();
			for(JsonNode columnNode : columnsNode)
				columns.add(new TextColumn(columnNode.path("column").asText(), TeradataColumnType.fromTypeName(columnNode.path("type").asText())));
			String tableName = fileName.substring(0, fileName.lastIndexOf("."));
			table2Headers.put(tableName, columns);
		}
		
		return table2Headers;
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

	public static List<DataSet> findDatasets(String directoryPath) throws IOException {
		Set<String> processedSubsetNames = new HashSet<>();
		List<DataSet> datasets = new ArrayList<>(20);
		File directory = new File(directoryPath);
		if(!directory.exists() || !directory.isDirectory())
			return datasets;
		
		File[] files = directory.listFiles();
		for (File file : files) {
			//database dump files are prefixed with
			//database name and table e.g. db_table.yyyy.csv
			String commonName = file.getName();
			commonName = commonName.substring(0, commonName.indexOf("."));
			
			//if there is an entry for the prefix all files were found already
			if (processedSubsetNames.contains(commonName)) continue;
			List<DataTable> subsets = new ArrayList<>(10);
			List<RowElement> columnNames = parseHeader(file);
			for (File f : files) {
				String fname = f.getName();
				if(!fname.startsWith(commonName)) continue;
				
				subsets.add(new FileDataTable(f, ";", fname, columnNames));
			}
			processedSubsetNames.add(commonName);	
			datasets.add(new DataSet(commonName, subsets, true));
		}
		
		return datasets;
	}
	
	private static List<RowElement> parseHeader(File fi) throws IOException {
		List<RowElement> header = new ArrayList<>();
		CSVParser reader = new CSVParser(
				new BufferedReader(new FileReader(fi)),
				CSVFormat.DEFAULT.withDelimiter(';').withRecordSeparator("\n")
		);
		
		//just read the first line with the column names
		for(CSVRecord record : reader) {
			IntStream
				.range(0, record.size())
				.forEach(i -> header.add(new SimpleRowElement(record.get(i), i, TeradataColumnType.UNKNOWN, record.get(i))));
			break;
		}
		reader.close();
		
		return header;
	}
	
	/**
	 * Reads column count from a two column csv
	 * E.g.:
	 * 		ACC_ADB_AVK_ADB_T_AM_EVO;20
	 * 		ACC_ADB_AVK_ADB_T_KH_Diagnose;18
	 * @param path file path
	 * @return table name to column count map
	 * @throws IOException
	 */
	public static Map<String,Integer> countColumnsInHeaderFiles(String path) throws IOException {
		Map<String,Integer> table2columnCount = new HashMap<>();
		BufferedReader reader = new BufferedReader(new FileReader(path));
		reader.lines()
			.filter(line -> line != null && !line.isEmpty())
			.map(line -> line.split(";"))
			.forEach(array -> table2columnCount.put(array[0], Integer.parseInt(array[1])));
		reader.close();
		return table2columnCount;
	}
	
	public static boolean areCredentialsCorrect(String user, String password, String url) {
		Connection conn = null;
		try {
			Class.forName("com.teradata.jdbc.TeraDriver");
			conn = DriverManager.getConnection(
					url,
					user,
					password
			);
			conn.close();
			return true;
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			try {
				if(conn != null && !conn.isClosed()) conn.close();
			} catch (SQLException e1) {
				e1.printStackTrace();
				return false;
			}
			return false;
		}
		return false;
	}
	
	public static ExecutorService createThreadPool(int size, boolean spawnDaemonThreads) {
		return Executors.newFixedThreadPool(
				size,
				new ThreadFactory() {
					
					@Override
					public Thread newThread(Runnable r) {
						Thread t = new Thread(r);
						t.setDaemon(spawnDaemonThreads);
						return t;
					}
				}
			);
	}
}
