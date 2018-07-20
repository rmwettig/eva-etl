package de.ingef.eva.tasks;

import java.io.IOException;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;

import de.ingef.eva.error.TaskExecutionException;
import de.ingef.eva.etl.Row;
import de.ingef.eva.utility.CsvWriter;
import lombok.extern.log4j.Log4j2;

/**
 * Writes database row stream to disk.
 * This task cannot be retried due to the stream.
 * 
 * @author Martin.Wettig
 *
 */
@Log4j2
public class WriteFileTask extends Task<Boolean> {

	private final CsvWriter writer;
	private final Stream<Row> rows;
	private final Predicate<Row> rowFilter;
	private final Function<Row, Row> rowTransformer;
	
	public WriteFileTask(String name, String description, CsvWriter writer, Stream<Row> rows, Predicate<Row> rowFilter, Function<Row, Row> rowTransformer) {
		super(name, description);
		this.writer = writer;
		this.rows = rows;
		this.rowFilter = rowFilter;
		this.rowTransformer = rowTransformer;
	}
	
	public WriteFileTask(CsvWriter writer, Stream<Row> rows, Predicate<Row> rowFilter, Function<Row, Row> rowTransformer) {
		this("WriteFileTask", "", writer, rows, rowFilter, rowTransformer);
	}

	@Override
	public Boolean execute() {
		rows
		.filter(rowFilter)
		.map(rowTransformer)
		.forEach(row -> {
			try {
				if(!writer.isNewFile()) {
					writeColumns(writer, row);
				} else {
					writeHeader(writer, row.getColumnName2Index());
					writeColumns(writer, row);
				}
			} catch (IOException e) {
				throw new TaskExecutionException("File creation failed.", e);
			}
		});
		try {
			writer.close();
		} catch (IOException e) {
			log.error("Could not close writer. {}", e);
			return false;
		}
		return true;
	}
	
	private void writeColumns(CsvWriter writer, Row row) throws IOException {
		row.getColumns()
		.stream()
		.map(e -> e.getContent())
		.forEachOrdered(value -> writer.addEntry(value));
		writer.writeLine();
	}

	private void writeHeader(CsvWriter writer, Map<String, Integer> columnName2Index) throws IOException {
		String[] header = new String[columnName2Index.size()];
		columnName2Index.forEach((name, index) -> header[index] = name);
		for(String h : header)
			writer.addEntry(h);
		writer.writeLine();
	}
}
