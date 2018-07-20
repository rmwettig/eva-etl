package de.ingef.eva.etl;

import java.io.IOException;
import java.nio.file.Path;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import de.ingef.eva.configuration.Configuration;
import de.ingef.eva.constant.OutputDirectory;
import de.ingef.eva.constant.OutputDirectory.DirectoryType;
import de.ingef.eva.data.RowElement;
import de.ingef.eva.data.SimpleRowElement;
import de.ingef.eva.data.TeradataColumnType;
import de.ingef.eva.query.Query;
import de.ingef.eva.services.ConnectionFactory;
import de.ingef.eva.services.TaskRunner;
import de.ingef.eva.tasks.SqlTask;
import de.ingef.eva.tasks.WriteFileTask;
import de.ingef.eva.utility.CsvWriter;
import de.ingef.eva.utility.IOManager;
import de.ingef.eva.utility.progress.ProgressBar;
import lombok.extern.log4j.Log4j2;

@Log4j2
public class ETLPipeline {
	
	public void run(Configuration configuration, Collection<Query> queries, List<Filter> filters, List<Transformer> transformers, IOManager ioManager, TaskRunner taskRunner, ConnectionFactory connectionFactory) {
		ProgressBar progress = new ProgressBar(queries.size());
		CountDownLatch countdown = new CountDownLatch(queries.size());
		Predicate<Row> rowFilter = createRowFilter(filters);
		Function<Row, Row> rowTransformer = createRowTransformer(transformers);
		for(Query q : queries) {
			startExport(q, taskRunner, connectionFactory, rowFilter, rowTransformer, ioManager, progress, countdown);
		}
		try {
			countdown.await(3, TimeUnit.DAYS);
		} catch (InterruptedException e) {
			log.error("Export tasks did not terminate normally. {}", e);
		}
	}
		
	/**
	 * creates asynchronous task to run the export
	 * @param q
	 * @param taskRunner
	 * @param connectionFactory
	 * @param filters
	 * @param transformers
	 * @param ioManager
	 * @param progressBar
	 * @param countdown
	 */
	private void startExport(Query q, TaskRunner taskRunner, ConnectionFactory connectionFactory, Predicate<Row> rowFilter, Function<Row, Row> rowTransformer, IOManager ioManager, ProgressBar progressBar, CountDownLatch countdown) {
		CsvWriter writer = createWriter(ioManager, q);
		taskRunner
			.run(new SqlTask("Export Query", q, connectionFactory, this::convertToRow), 3)
			.thenCompose(rowStream -> createStreamProcessor(taskRunner, rowFilter, rowTransformer, writer, rowStream))
			.thenAccept(arg -> {
				makeProgress(progressBar, countdown);
			})
			.exceptionally(e -> {
				log.error("Export error occurred for query: DB: {}, Dataset: {}, Table: {}, Slice: {}, Query: {}", q.getDbName(), q.getDatasetName(), q.getTableName(), q.getSliceName(), q.getQuery(), e);
				makeProgress(progressBar, countdown);
				return null;
			});
	}

	/**
	 * indicates that a pipeline has finished
	 * @param progressBar
	 * @param countdown
	 */
	private void makeProgress(ProgressBar progressBar, CountDownLatch countdown) {
		progressBar.increase();
		countdown.countDown();
	}
	
	/**
	 * creates a function that applies filters to rows
	 * @param filters
	 * @return
	 */
	private Predicate<Row> createRowFilter(List<Filter> filters) {
		return (Row row) -> {
			for(Filter f : filters) {
				if(!f.isValid(row)) {
					log.error("Invalid row in table '{}'. Filter '{}' failed. [{}]",
							row.getTable(),
							f.getName(),
							row.getColumns().stream().map(e -> e.getContent()).collect(Collectors.joining(", ")));
					return false;
				}
			}
			return true;
		};
	}
	
	/**
	 * creates a function that applies transformers to row
	 * @param transformers
	 * @return
	 */
	private Function<Row, Row> createRowTransformer(List<Transformer> transformers) {
		return (Row row) -> {
			Row transformed = row;
			for(Transformer t : transformers) {
				transformed = t.transform(transformed);
			}
			return transformed;
		};
	}
	
	/**
	 * starts a file writer task to save rows to disk
	 * @param taskRunner
	 * @param rowFilter
	 * @param rowTransformer
	 * @param writer
	 * @param rowStream
	 * @return CompletableFuture without return value
	 */
	private CompletableFuture<Boolean> createStreamProcessor(TaskRunner taskRunner, Predicate<Row> rowFilter, Function<Row, Row> rowTransformer, CsvWriter writer, Stream<Row> rowStream) {
		return taskRunner.run(new WriteFileTask(writer, rowStream, rowFilter, rowTransformer));
	}

	/**
	 * evaluates a result set from a database and creates a row object
	 * @param result
	 * @return
	 */
	private Row convertToRow(ResultSet result) {
		try {
			ResultSetMetaData metaData = result.getMetaData();
			int columnCount = metaData.getColumnCount();
		
			List<RowElement> columns = new ArrayList<>(columnCount);
			for(int columnIndex = 1; columnIndex <= columnCount; columnIndex++) {
				//perform null checks on raw data from database
				//otherwise threads stall and program does not terminate
				String value = result.getString(columnIndex) == null ? "" : cleanValue(result.getString(columnIndex));
				TeradataColumnType type = TeradataColumnType.fromTypeName(metaData.getColumnLabel(columnIndex));
				columns.add(new SimpleRowElement(value, type));
			}
			Map<String,Integer> columnNames2Index = createColumnIndexLookup(metaData);
			
			Row row = new Row();
			row.setColumns(columns);
			row.setColumnName2Index(columnNames2Index);
			return row;
		} catch (SQLException e) {
			log.error("Could not convert result set to row. {}", e);
		}
		Row row = new Row();
		row.setDb("");
		row.setTable("");
		row.setColumnName2Index(Collections.emptyMap());
		row.setColumns(Collections.emptyList());
		return row;
	}
	
	/**
	 * removes semicolons and quotation marks from the given string
	 * @param value
	 * @return a cleaned string
	 */
	private String cleanValue(String value) {
		return value
				.replaceAll(";", "_")
				.replaceAll("\"","")
				.trim();
	}

	/**
	 * creates a gzip csv writer with the expected output file
	 * @param ioManager
	 * @param q
	 * @return
	 */
	private CsvWriter createWriter(IOManager ioManager, Query q) {
		String dbShortName = createDbShortName(q.getDbName());
		Path root = ioManager.createSubdirectories(DirectoryType.CACHE, dbShortName, q.getDatasetName());
		String fileName = createOutputFileName(q);
		try {
			return CsvWriter.createGzipWriter(root.resolve(fileName));
		} catch (IOException e) {
			log.error("Could not create writer for file '{}'. {}", fileName, e);
		}
		return null;
	}

	private String createOutputFileName(Query q) {
		return q.getDbName() + "_" + q.getTableName()  + "." + q.getSliceName() + OutputDirectory.CACHE_FILE_EXTENSION;
	}

	private String createDbShortName(String dbName) {
		if(dbName.contains("_"))
			return dbName.split("_")[1];
		return dbName;
	}
	
	private Map<String, Integer> createColumnIndexLookup(ResultSetMetaData metaData) throws SQLException {
		int count = metaData.getColumnCount();
		Map<String,Integer> map = new HashMap<>(count);
		for(int i = 0; i < count; i++) {
			map.put(metaData.getColumnLabel(i + 1).toLowerCase(), i);
		}
		return map;
	}
}
