package de.ingef.eva.datasource.sql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import de.ingef.eva.data.DataTable;
import de.ingef.eva.datasource.DataSource;
import de.ingef.eva.configuration.Configuration;
import lombok.AllArgsConstructor;
import lombok.extern.log4j.Log4j2;

@Log4j2 @AllArgsConstructor
public class SqlDataSource implements DataSource {

	private final String query;
	private final String name;
	private final Configuration config;
	
	@Override
	public DataTable fetchData() {
		try {
			Class.forName("com.teradata.jdbc.TeraDriver");
			try {
				Connection conn = DriverManager.getConnection(
						config.getFullConnectionUrl(),
						config.getUser(),
						config.getPassword()
				);
				Statement statement = conn.createStatement();
				log.info("Executing query: {}", query);
				ResultSet result = statement.executeQuery(query);
				return new SqlDataTable(result, result.getMetaData(), name);
			} catch (SQLException e) {
				log.error("Could not open connection or creating query.\n\tReason: {}", e.getMessage());
			  }
			} catch (ClassNotFoundException e) {
			log.error("Did not found Teradata JDBC driver.");
		}
		
		return null;
	}

}
