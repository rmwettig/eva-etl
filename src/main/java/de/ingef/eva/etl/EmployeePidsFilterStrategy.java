package de.ingef.eva.etl;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonTypeName;

import de.ingef.eva.configuration.Configuration;
import de.ingef.eva.configuration.FilterStrategyType;
import de.ingef.eva.constant.Templates;
import de.ingef.eva.data.RowElement;
import de.ingef.eva.data.SimpleRowElement;
import de.ingef.eva.data.TeradataColumnType;
import de.ingef.eva.utility.Helper;
import lombok.extern.log4j.Log4j2;

@JsonTypeName(value="EMPLOYEE_PIDS")
@Log4j2
public class EmployeePidsFilterStrategy extends FilterStrategy {

	@JsonIgnore
	private SetFilterStrategy strategy;
	
	public EmployeePidsFilterStrategy() {
		super(FilterStrategyType.UNIQUE_SET);
	}
	
	@Override
	public boolean isValid(String value) {
		return strategy.isValid(value);
	}

	@Override
	public void initialize(Configuration config) {
		Connection conn = Helper.createConnectionWithErrorHandling(config.getUser(), config.getPassword(), config.getFullConnectionUrl());
		List<Row> pidList = new SqlRunner().run(conn, "ADB", "AVK_ADB_PID_AKTUELL", Templates.Filter.SELECT_EMPLOYEE_PIDS, this::extractColumnValue, this::createIndexMap);
		try {
			conn.close();
		} catch (SQLException e) {
			log.error("Could not close connection for employee filter. {}", e);
		}
		strategy = new SetFilterStrategy(
			pidList
				.stream()
				.map(row -> row.getColumns().get(0))
				.map(rowElement -> rowElement.getContent().trim())
				.collect(Collectors.toSet())
		);
	}
		
	private List<RowElement> extractColumnValue(ResultSet result) {
		try {
			return Arrays.asList(new SimpleRowElement(result.getString(1).trim(), TeradataColumnType.VARCHAR));
		} catch (SQLException e) {
			log.error("Could not extract pid value for employee filter. {}", e);
		}
		return Collections.emptyList();
	}
	
	private Map<String, Integer> createIndexMap(ResultSetMetaData metaData) {
		try {
			Map<String, Integer> index = new HashMap<>(1);
			index.put(metaData.getColumnName(1), 0);
			return index;
		} catch (SQLException e) {
			log.error("Could not create column index. {}", e);
		}
		return Collections.emptyMap();
	}
}
