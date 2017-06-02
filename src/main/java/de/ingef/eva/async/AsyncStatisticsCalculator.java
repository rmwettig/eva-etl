package de.ingef.eva.async;

import de.ingef.eva.configuration.Configuration;
import de.ingef.eva.datasource.DataTable;
import de.ingef.eva.datasource.sql.SqlDataSource;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class AsyncStatisticsCalculator implements Runnable {
	private final String name;
	private final String query;
	private final Configuration configuration;
	
	@Override
	public void run() {
		DataTable data = new SqlDataSource(query, name, configuration).fetchData();
		
	}

}
