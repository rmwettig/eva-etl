package de.ingef.eva.etl;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import de.ingef.eva.data.RowElement;
import de.ingef.eva.data.SimpleRowElement;
import de.ingef.eva.data.TeradataColumnType;
import lombok.RequiredArgsConstructor;

/**
 * Appends columns loaded from a file to the end of the row
 * @author Martin.Wettig
 *
 */
@RequiredArgsConstructor
public class DynamicColumnAppenderTransformer extends Transformer {

	private final String db;
	private final String table;
	private final String keyColumn;
	private final Map<String,List<RowElement>> additionalColumns;
	private final List<RowElement> additionalColumnNames;
	
	@Override
	public Row transform(Row row) {
		if((db != null || !db.isEmpty()) && row.getDb().equalsIgnoreCase(db)) {
			return process(row);
		}
		
		if((table != null || !table.isEmpty()) && row.getTable().equalsIgnoreCase(table)) {
			return process(row);
		}
		
		return row;
	}

	private Row process(Row row) {
		if(!row.getColumnName2Index().containsKey(keyColumn))
			return row;
		int keyIndex = row.getColumnName2Index().get(keyColumn);
		String keyValue = row.getColumns().get(keyIndex).getContent();
		List<RowElement> columns = transformColumns(row.getColumns(), keyValue);
		Map<String,Integer> columnIndices = transformColumnIndices(row.getColumnName2Index());
		return new Row(row.getDb(), row.getTable(), columns, columnIndices);
	}

	private Map<String, Integer> transformColumnIndices(Map<String, Integer> columnName2Index) {
		Map<String,Integer> transformedIndices = new HashMap<String,Integer>(columnName2Index.size() + additionalColumnNames.size());
		columnName2Index.forEach((column,index) -> transformedIndices.put(column, index));
		IntStream
			.range(0, additionalColumnNames.size())
			//first empty position is at columnName2Index.size() and is pushed forwards by adding the index
			.forEachOrdered(i -> transformedIndices.put(additionalColumnNames.get(i).getContent(), i + columnName2Index.size()));
		return transformedIndices;
	}

	private List<RowElement> transformColumns(List<RowElement> columns, String keyValue) {
		List<RowElement> transformedColumns = new ArrayList<>(columns.size() + additionalColumns.size());
		transformedColumns.addAll(columns);
		//if there is no data for the key add empty elements to pad row size
		if(!additionalColumns.containsKey(keyValue)) {
			IntStream
				.range(0, additionalColumnNames.size())
				.forEach(i -> transformedColumns.add(new SimpleRowElement("", TeradataColumnType.CHARACTER)));
		} else {
			transformedColumns.addAll(additionalColumns.get(keyValue));
		}
		return transformedColumns;
	}
	
	public static Transformer of(String targetDb, String targetTable, String keyColumn, String source) throws IOException {
		BufferedReader reader = Files.newBufferedReader(Paths.get(source));
		List<RowElement> columnNames = readHeader(reader);
		Map<String,List<RowElement>> additionalColumns = readColumns(reader);
		
		return new DynamicColumnAppenderTransformer(targetDb, targetTable, keyColumn, additionalColumns, columnNames);
	}
	
	private static Map<String, List<RowElement>> readColumns(BufferedReader reader) throws IOException {
		String line = null;
		Map<String,List<RowElement>> additionalColumns = new HashMap<>();
		while((line = reader.readLine()) != null) {
			if(line.isEmpty())
				continue;
			String[] arr = line.split(";");
			if(arr[0] == null || arr[0].isEmpty())
				continue;
			//first field is the key
			List<RowElement> columns = new ArrayList<>(arr.length - 1);
			IntStream.range(1, arr.length).forEachOrdered(i -> columns.add(new SimpleRowElement(arr[i], TeradataColumnType.CHARACTER)));
			additionalColumns.put(arr[0], columns);
		}
		
		return additionalColumns;
	}

	private static List<RowElement> readHeader(BufferedReader reader) throws IOException {
		String header = reader.readLine();
		String[] names = header.split(";");
		List<RowElement> columnNames = new ArrayList<>(names.length);
		for(String name : names)
			columnNames.add(new SimpleRowElement(name, TeradataColumnType.CHARACTER));
		return columnNames;
	}
}
