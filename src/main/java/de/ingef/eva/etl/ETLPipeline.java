package de.ingef.eva.etl;

import java.io.IOException;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import de.ingef.eva.configuration.Configuration;
import de.ingef.eva.constant.OutputDirectory.DirectoryType;
import de.ingef.eva.data.RowElement;
import de.ingef.eva.data.SimpleRowElement;
import de.ingef.eva.data.TeradataColumnType;
import de.ingef.eva.query.Query;
import de.ingef.eva.utility.CsvWriter;
import de.ingef.eva.utility.Helper;
import de.ingef.eva.utility.IOManager;
import de.ingef.eva.utility.progress.ProgressBar;
import lombok.extern.log4j.Log4j2;

@Log4j2
public class ETLPipeline {
	
	public void run(Configuration configuration, Collection<Query> queries, List<Filter> filters, List<Transformer> transformers, IOManager ioManager) {
		ExecutorService threadPool = Helper.createThreadPool(configuration.getThreadCount(), true);
		String user = configuration.getUser();
		String url = configuration.getFullConnectionUrl();
		String password = configuration.getPassword();
		ProgressBar progress = new ProgressBar(queries.size());
		for(Query q : queries) {
			startExport(q, threadPool, url, user, password, filters, transformers, ioManager, progress);
		}
		
		threadPool.shutdown();
		try {
			threadPool.awaitTermination(3, TimeUnit.DAYS);
		} catch (InterruptedException e) {
			log.error("Export tasks did not terminate normally. {}", e);
		}
	}
	
	private Connection createConnection(String url, String user, String password) throws ClassNotFoundException, SQLException {
		Class.forName("com.teradata.jdbc.TeraDriver");
		return DriverManager.getConnection(url, user, password);
	}
	
	private void startExport(Query q, ExecutorService threadPool, String url, String user, String password, List<Filter> filters, List<Transformer> transformers, IOManager ioManager, ProgressBar progressBar) {
		CompletableFuture.supplyAsync(
				() -> {
					Connection conn = null;
					PreparedStatement ps = null;
					ResultSet rs = null;
					CsvWriter writer = null;
					try {
						Class.forName("com.teradata.jdbc.TeraDriver");
						conn = DriverManager.getConnection(url, user, password);
						ps = conn.prepareStatement(q.getQuery());
						log.info("Executing query: {}", q.getQuery().replaceAll("\n", " "));
						rs = ps.executeQuery();
						
						if(!conn.isValid(5)) {
							log.info("Retrying query for : '{}'. Query: {}", q.getName(), q.getQuery().replaceAll("\n", " "));
							rs.close();
							ps.close();
							conn.close();
							conn = createConnection(url, user, password);
							ps = conn.prepareStatement(q.getQuery());
							rs = ps.executeQuery();
						}
						
						ResultSetMetaData metaData = rs.getMetaData();
						int columnCount = metaData.getColumnCount();
						writer = createWriter(ioManager, q);
						boolean wasHeaderWritten = false;
						while(rs.next()) {
							List<RowElement> columns = new ArrayList<>(columnCount);
							for(int columnIndex = 1; columnIndex <= columnCount; columnIndex++) {
								//perform null checks on raw data from database
								//otherwise threads stall and program does not terminate
								String value = rs.getString(columnIndex) == null ? "" : cleanValue(rs.getString(columnIndex));
								TeradataColumnType type = TeradataColumnType.fromTypeName(metaData.getColumnLabel(columnIndex));
								columns.add(new SimpleRowElement(value, type));
							}
							Map<String,Integer> columnNames2Index = createColumnIndexLookup(metaData);
							
							Row row = new Row(q.getDbName(), q.getTableName(), columns, columnNames2Index);
							if(!isRowValid(filters, row))
								continue;
							Row transformedRow = transformRow(transformers, row);
							if(!wasHeaderWritten) {
								writeHeader(writer, transformedRow.getColumnName2Index());
								wasHeaderWritten = true;
							}
							writeToFile(writer, transformedRow);
						}
						progressBar.increase();
					} catch(SQLException e) {
						throw new RuntimeException(e);
					} catch (ClassNotFoundException e) {
						throw new RuntimeException("Could not find Teradata driver", e);
					} catch (IOException e1) {
						throw new RuntimeException("Could not create output file for query: '" + q.getName() +"'", e1);
					} finally {
						try {
							if(writer != null)
								writer.close();
							if(rs != null && !rs.isClosed())
								rs.close();
							if(ps != null && !ps.isClosed())
								ps.close();
							if(conn != null && !conn.isClosed())
								conn.close();
							
						} catch(SQLException e) {
							throw new RuntimeException("Could not close sql resources", e);
						} catch (IOException e1) {
							throw new RuntimeException("Could not close file output", e1);
						}
					}
					return null;
				},
				threadPool
		).exceptionally(e -> {
			log.error("Export error occurred for query: DB: {}, Table: {}, Slice: {}", q.getDbName(), q.getTableName(), q.getSliceName(), e);
			return null;
		});
	}

	private String cleanValue(String value) {
		return value
				.replaceAll(";", "_")
				.replaceAll("\"","")
				.trim();
	}

	private void writeToFile(CsvWriter writer, Row transformedRow) throws IOException {
		List<RowElement> transformedColumns = transformedRow.getColumns();		
		for(RowElement e : transformedColumns)
			writer.addEntry(e.getContent());
		writer.writeLine();
	}

	private void writeHeader(CsvWriter writer, Map<String, Integer> columnName2Index) throws IOException {
		String[] header = new String[columnName2Index.size()];
		columnName2Index.forEach((name, index) -> header[index] = name);
		for(String h : header)
			writer.addEntry(h);
		writer.writeLine();
	}

	private CsvWriter createWriter(IOManager ioManager, Query q) throws IOException {
		String dbShortName = createDbShortName(q.getDbName());
		Path root = ioManager.createSubdirectories(DirectoryType.CACHE, dbShortName, q.getDatasetName());
		String fileName = createOutputFileName(q); 
		CsvWriter writer = new CsvWriter(root.resolve(fileName).toFile());
		writer.open();
		return writer;
	}

	private String createOutputFileName(Query q) {
		return q.getDbName() + "_" + q.getTableName()  + "." + q.getSliceName() + ".csv";
	}

	private String createDbShortName(String dbName) {
		if(dbName.contains("_"))
			return dbName.split("_")[1];
		return dbName;
	}

	private Row transformRow(List<Transformer> transformers, Row row) {
		Row transformed = row;
		for(Transformer t : transformers) {
			transformed = t.transform(transformed);
		}
		return transformed;
	}

	private boolean isRowValid(List<Filter> filters, Row row) {
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
