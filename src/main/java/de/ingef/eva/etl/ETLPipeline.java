package de.ingef.eva.etl;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import de.ingef.eva.configuration.Configuration;
import de.ingef.eva.data.RowElement;
import de.ingef.eva.data.SimpleRowElement;
import de.ingef.eva.data.TeradataColumnType;
import de.ingef.eva.query.Query;

@Log4j2
public class ETLPipeline {
	
	public void run(Configuration configuration, Collection<Query> queries, List<FilterBase> filters, List<Transformer> transformers) {
		ExecutorService threadPool = Executors.newFixedThreadPool(configuration.getThreadCount());
		CompletableFuture.supplyAsync(
				() -> {
					Connection conn = null;
					PreparedStatement ps = null;
					ResultSet rs = null;
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
						System.out.println("\tFetching...");
						while(rs.next()) {
							List<RowElement> columns = new ArrayList<>(columnCount);
							for(int columnIndex = 1; columnIndex <= columnCount; columnIndex++) {
								//perform null checks on raw data from database
								//otherwise threads stall and program does not terminate
								String value = rs.getString(columnIndex) == null ? "" : rs.getString(columnIndex).trim();
								TeradataColumnType type = TeradataColumnType.fromTypeName(metaData.getColumnLabel(columnIndex));
								columns.add(new SimpleRowElement(value, type));
							}
							Map<String,Integer> columnNames2Index = createColumnIndexLookup(metaData);
							
							output.put(new Row(q.getDBName(), q.getTableName(), columns, columnNames2Index));
						}
						System.out.println("\tDone.");
					} catch(SQLException e) {
						throw new RuntimeException(e);
					} catch (ClassNotFoundException e) {
						throw new RuntimeException("Could not find Teradata driver", e);
					} catch (InterruptedException e) {
						throw new RuntimeException(e);
					} finally {
						try {
							if(rs != null && !rs.isClosed())
								rs.close();
							if(ps != null && !ps.isClosed())
								ps.close();
							if(conn != null && !conn.isClosed())
								conn.close();
						} catch(SQLException e) {
							throw new RuntimeException("Could not close sql resources", e);
						}
					}
					return null;
				},
				threadPool
		).exceptionally(e -> {
			log.error("Export error occurred: {}", e);
			return null;
		});
	}
}
