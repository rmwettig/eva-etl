package de.ingef.eva.etl.stage;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import de.ingef.eva.data.RowElement;
import de.ingef.eva.etl.Row;
import de.ingef.eva.query.Query;
import lombok.Getter;
import lombok.extern.log4j.Log4j2;

@Log4j2
public class Export {
	
	private ExecutorService threadPool;
	@Getter
	private BlockingQueue<Row> output;
	private final Row pill;
	
	public Export (Row poisonPill) {
		pill = poisonPill;
	}
	
	public boolean initialize(int queueSize, int threadCount) {
		threadPool = Executors.newFixedThreadPool(threadCount);
		output = new ArrayBlockingQueue<>(queueSize);
	}
	
	public void start(Collection<Query> queries, String url, String user, String password) {
		List<CompletableFuture<Object>> futures = new ArrayList<>(queries.size());
		for(Query q : queries) {
			CompletableFuture<Object> cf = createTasks(q, url, user, password);
			futures.add(cf);
		}
		createTerminationSignal(futures);
	}
	
	private void createTerminationSignal(List<CompletableFuture<Object>> futures) {
		CompletableFuture<Object>[] arr = new CompletableFuture[futures.size()];
		CompletableFuture.allOf(futures.toArray(arr)).thenAcceptAsync(v -> {
			try {
				output.put(pill);
				System.out.println("Send poison pill...");
			} catch (InterruptedException e) {
				throw new RuntimeException(e);
			}
		})
		.exceptionally(e -> {
			log.error("Could not put termination signal. {}", e);
			return null;
		});
	}

	private CompletableFuture<Object> createTasks(Query q, String url, String user, String password) {
		return CompletableFuture.supplyAsync(
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
							log.info("Retrying query for : '{}'. Query: {}", query.getName(), query.getQuery().replaceAll("\n", ""));
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
								columns.add(new RowElement(value, metaData.getColumnLabel(columnIndex)));
							}
							output.put(new Row(q.getDBName(), q.getTableName(), columns));
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
	
	private Connection createConnection(String url, String user, String password) throws ClassNotFoundException, SQLException {
		Class.forName("com.teradata.jdbc.TeraDriver");
		return DriverManager.getConnection(url, user, password);
	}
}

