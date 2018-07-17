package de.ingef.eva.etl;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import de.ingef.eva.configuration.Configuration;
import de.ingef.eva.configuration.append.AppendConfiguration;
import de.ingef.eva.data.RowElement;
import de.ingef.eva.data.SimpleRowElement;
import de.ingef.eva.data.TeradataColumnType;
import de.ingef.eva.utility.CsvReader;
import lombok.extern.log4j.Log4j2;

@Log4j2
public class HashTransformer extends Transformer {

	private static final String OUTPUT_COLUMN_NAME = "pid_hash";
	private final Map<String, String> pid2Hash;	
	
	public HashTransformer(String db, String table, Map<String, String> hashMapping) {
		super(db, table);
		pid2Hash = hashMapping;
	}
		
	@Override
	public Row transform(Row row) {
		if(!canProcessRow(row.getDb(), row.getTable()))
			return row;
		if(!row.getColumnName2Index().containsKey("pid")) {
			log.warn("Missing pid column in {}.{}", row.getDb(), row.getTable());
			return row;
		}
		int pidColumnIndex = row.getColumnName2Index().get("pid");
		String hash = pid2Hash.getOrDefault(row.getColumns().get(pidColumnIndex).getContent(), "");
		
		return createNewRow(row, hash);
	}

	private Row createNewRow(Row row, String hash) {
		Map<String, Integer> transformedIndices = transformIndices(row.getColumnName2Index());
		List<RowElement> transformedColumns = transformColumns(row.getColumns(), hash);
		return new Row(row.getDb(), row.getTable(), transformedColumns, transformedIndices);
	}

	private List<RowElement> transformColumns(List<RowElement> columns, String value) {
		List<RowElement> transformedColumns = new ArrayList<>(columns.size() + 1);
		transformedColumns.addAll(columns);
		transformedColumns.add(new SimpleRowElement(value, TeradataColumnType.CHARACTER));
		return transformedColumns;
	}

	private Map<String, Integer> transformIndices(Map<String, Integer> columnName2Index) {
		Map<String, Integer> transformed = new HashMap<>(columnName2Index);
		transformed.put(OUTPUT_COLUMN_NAME, columnName2Index.size());
		return transformed;
	}

	public static Transformer of(Configuration config, AppendConfiguration transformerConfig) {
		try {
			CsvReader reader = CsvReader.createReader(config.getHashing().getHashFile());
			return new HashTransformer(transformerConfig.getTargetDb(), transformerConfig.getTargetTable(), reader.lines().map(mapping -> mapping).collect(Collectors.toMap(mapping -> mapping.get(0), mapping -> mapping.get(1))));
		} catch (FileNotFoundException e) {
			log.error("Could not find hash file '{}'. {}", config.getHashing().getHashFile(), e);
			return new Transformer.NOPTransformer();
		} catch (IOException e) {
			log.error("IO Error: {}", e);
			return new Transformer.NOPTransformer();
		}
	}
}
