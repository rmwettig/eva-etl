package de.ingef.eva.etl;

import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import de.ingef.eva.configuration.append.AppendConfiguration;
import de.ingef.eva.constant.OutputDirectory;
import de.ingef.eva.data.RowElement;
import de.ingef.eva.data.SimpleRowElement;
import de.ingef.eva.data.TeradataColumnType;
import lombok.extern.log4j.Log4j2;

@Log4j2
public class DynamicColumnAppender extends Transformer {

	private String keyColumn;
	private List<RowElement> columnNames;
	private Map<String, List<RowElement>> newColumns;
	
	public DynamicColumnAppender(String db, String table, String keyName, List<RowElement> header, Map<String, List<RowElement>> key2Columns) {
		super(db, table);
		newColumns = key2Columns;
		columnNames = header;
		keyColumn = keyName;
	}

	@Override
	public Row transform(Row row) {
		if(!canProcessRow(row.getDb(), row.getTable()))
			return row;
		Map<String, Integer> transformedIndices = transformIndices(row.getColumnName2Index());
		List<RowElement> transformedColumns;
		if(row.getColumnName2Index().containsKey(keyColumn)) {
			int keyIndex = row.getColumnName2Index().get(keyColumn);
			String key = row.getColumns().get(keyIndex).getContent();
			transformedColumns = transformColumns(key, row.getColumns());
		} else {
			transformedColumns = emptyColumns();
		}
		
		return new Row(row.getDb(), row.getTable(), transformedColumns, transformedIndices);
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
			Map<String, List<RowElement>> newColumns = createMappings(lines.subList(1, lines.size()));
			List<RowElement> header = createColumnHeaders(lines.get(0));
			return new DynamicColumnAppender(config.getTargetDb(), config.getTargetTable(), config.getKeyColumn(), header, newColumns);
		} catch (IOException e) {
			log.error("Could not read mapping file: {}", e);
			return new Transformer.NOPTransformer();
		}		
	}
	
	private static Map<String, List<RowElement>> createMappings(List<String> mappings) {
		return mappings
				.stream()
				.map(lines -> {
					String[] columns = lines.split(";");
					List<RowElement> re = new ArrayList<>(columns.length);
					for(String column : columns)
						re.add(new SimpleRowElement(column, TeradataColumnType.VARCHAR));
					return re;
				})
				.collect(Collectors.toMap(columns -> columns.get(0).getContent(), columns -> columns.subList(1, columns.size())));
	}
	
	private static List<RowElement> createColumnHeaders(String header) {
		String[] columns = header.split(";");
		List<RowElement> headerColumns = new ArrayList<>(columns.length - 1);
		for(int i = 1; i < columns.length; i++)
			headerColumns.add(new SimpleRowElement(columns[i], TeradataColumnType.VARCHAR));
		return headerColumns;
	}
}
