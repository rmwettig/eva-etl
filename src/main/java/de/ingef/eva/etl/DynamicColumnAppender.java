package de.ingef.eva.etl;

import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import de.ingef.eva.configuration.append.AppendConfiguration;
import de.ingef.eva.constant.OutputDirectory;
import de.ingef.eva.data.RowElement;
import de.ingef.eva.data.SimpleRowElement;
import de.ingef.eva.data.TeradataColumnType;
import lombok.extern.log4j.Log4j2;

@Log4j2
public class DynamicColumnAppender extends Transformer {

	private static final String KEY_VALUE_DELIMITER = "|";
	private List<String> keyColumns;
	private List<RowElement> columnNames;
	private Map<String, List<RowElement>> newColumns;
	
	public DynamicColumnAppender(String db, String table, List<String> keyNames, List<RowElement> header, Map<String, List<RowElement>> key2Columns) {
		super(db, table);
		newColumns = key2Columns;
		columnNames = header;
		keyColumns = keyNames;
	}

	@Override
	public Row transform(Row row) {
		if(!canProcessRow(row.getDb(), row.getTable()))
			return row;
		Map<String, Integer> transformedIndices = transformIndices(row.getColumnName2Index());
		List<RowElement> transformedColumns;
		
		if(row.getColumnName2Index().keySet().containsAll(keyColumns)) {
			transformedColumns = transformColumns(createOrderedCombinedKey(row), row.getColumns());
		} else {
			transformedColumns = emptyColumns();
		}
		
		return new Row(row.getDb(), row.getTable(), transformedColumns, transformedIndices);
	}

	private String createOrderedCombinedKey(Row row) {
		return keyColumns
				.stream()
				.map(k -> row.getColumnName2Index().get(k))
				.map(index -> row.getColumns().get(index).getContent())
				.collect(Collectors.joining(KEY_VALUE_DELIMITER));
	}

	private List<RowElement> transformColumns(String key, List<RowElement> columns) {
		List<RowElement> transformed = new ArrayList<>(columns);
		if(newColumns.containsKey(key)) {
			transformed.addAll(newColumns.get(key));
		} else {
			transformed.addAll(emptyColumns());
		}
		return transformed;
	}

	private List<RowElement> emptyColumns() {
		return columnNames
				.stream()
				.map(header -> new SimpleRowElement("", TeradataColumnType.VARCHAR))
				.collect(Collectors.toList());
	}
	
	private Map<String, Integer> transformIndices(Map<String, Integer> columnName2Index) {
		Map<String, Integer> transformed = new HashMap<>(columnName2Index);
		int nextIndex = columnName2Index.size();
		for(RowElement columnName : columnNames)
			transformed.put(columnName.getContent(), nextIndex++);
		return transformed;
	}

	public static Transformer of(AppendConfiguration config) {
		try {
			List<String> lines = Files.newBufferedReader(config.getFile(), OutputDirectory.DATA_CHARSET).lines().collect(Collectors.toList());
			int keyColumnCount = config.getKeyColumns().size();
			Map<String, List<RowElement>> newColumns = createMappings(lines.subList(1, lines.size()), keyColumnCount);
			List<RowElement> header = createColumnHeaders(lines.get(0), keyColumnCount);
			return new DynamicColumnAppender(config.getTargetDb(), config.getTargetTable(), config.getKeyColumns(), header, newColumns);
		} catch (IOException e) {
			log.error("Could not read mapping file: {}", e);
			return new Transformer.NOPTransformer();
		}		
	}
	
	private static Map<String, List<RowElement>> createMappings(List<String> mappings, int keyColumnCount) {
		Function<? super List<RowElement>, String> keyCreator = list -> {
			return IntStream
						.range(0, keyColumnCount)
						.mapToObj(index -> list.get(index).getContent())
						.collect(Collectors.joining(KEY_VALUE_DELIMITER));
		};
		return mappings
				.stream()
				.map(lines -> {
					String[] columns = lines.split(";");
					List<RowElement> re = new ArrayList<>(columns.length);
					for(String column : columns)
						re.add(new SimpleRowElement(column, TeradataColumnType.VARCHAR));
					return re;
				})
				.collect(Collectors.toMap(keyCreator, columns -> columns.subList(keyColumnCount, columns.size())));
	}
	
	private static List<RowElement> createColumnHeaders(String header, int keyColumnCount) {
		String[] columns = header.split(";");
		List<RowElement> headerColumns = new ArrayList<>(columns.length - 1);
		for(int i = keyColumnCount; i < columns.length; i++)
			headerColumns.add(new SimpleRowElement(columns[i], TeradataColumnType.VARCHAR));
		return headerColumns;
	}
}
