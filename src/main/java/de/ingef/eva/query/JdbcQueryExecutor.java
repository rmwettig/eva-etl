package de.ingef.eva.query;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.concurrent.CountDownLatch;

import de.ingef.eva.configuration.Configuration;
import de.ingef.eva.constant.OutputDirectory;
import de.ingef.eva.data.DataTable;
import de.ingef.eva.error.QueryExecutionException;
import de.ingef.eva.utility.CsvWriter;
import lombok.extern.log4j.Log4j2;

@Log4j2
public class JdbcQueryExecutor implements QueryExecutor<Query> {

	private Configuration configuration;
	private CountDownLatch cdl;
		
	public JdbcQueryExecutor(Configuration config, CountDownLatch cdl) {
		configuration = config;
		this.cdl = cdl;
	}
	
	@Override
	public DataTable execute(Query query) throws QueryExecutionException {
		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			CsvWriter writer = createOutput(query.getName());
			writer.open();
			conn = createConnection();
			ps = conn.prepareStatement(query.getQuery());
			log.info("Executing query for : '{}'. Query: {}", query.getName(), query.getQuery().replaceAll("\n", ""));
			rs = ps.executeQuery();
			if(!conn.isValid(5)) {
				log.info("Retrying query for : '{}'. Query: {}", query.getName(), query.getQuery().replaceAll("\n", ""));
				rs.close();
				ps.close();
				conn.close();
				conn = createConnection();
				ps = conn.prepareStatement(query.getQuery());
				rs = ps.executeQuery();
			}
			ResultSetMetaData meta = rs.getMetaData();
			int columnCount = meta.getColumnCount();
			while(rs.next()) {
				for(int i = 1;  i <= columnCount; i++) {
					String value = rs.getString(i);
					if(value == null) {
						writer.addEntry("");
						continue;
					}
					value = value.replaceAll("\n", "")
								.replaceAll("\r", "")
								.replaceAll(";", "_");
					writer.addEntry(value.trim());					
				}
				writer.writeLine();
			}
			writer.close();
			cdl.countDown();
			
		} catch (SQLException e) {
			throw new QueryExecutionException("Could not run query '" + query.getName() + "'", e);
		} catch (ClassNotFoundException e) {
			throw new QueryExecutionException("Did not found Teradata JDBC driver.", e);
		} catch (IOException e) {
			throw new QueryExecutionException("Could not create raw output for '"+ query.getName() + "'", e);
		} finally {
			try {
				if(rs != null && !rs.isClosed())
					rs.close();
				if(ps != null && !ps.isClosed())
					ps.close();
				if(conn != null && !conn.isClosed())
					conn.close();
			} catch (SQLException e) {
				throw new QueryExecutionException("Could not close Sql resources.", e);
			}
		}
		return null;
	}

	private CsvWriter createOutput(String queryName) throws IOException {
		Path p = Paths.get(configuration.getOutputDirectory(), OutputDirectory.RAW);
		if(!Files.exists(p)) {
			Files.createDirectories(p);
		}
		return new CsvWriter(p.resolve(queryName + ".csv").toFile());
	}

	private Connection createConnection() throws ClassNotFoundException, SQLException {
		Class.forName("com.teradata.jdbc.TeraDriver");
		return DriverManager.getConnection(configuration.getFullConnectionUrl(), configuration.getUser(), configuration.getPassword());
	}

}
