package de.ingef.eva.etl;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import de.ingef.eva.data.RowElement;
import lombok.extern.log4j.Log4j2;

/**
 * Encapsulates sql execution and conversion to row objects
 * @author Martin.Wettig
 *
 */
@Log4j2
public class SqlRunner {
	
	public List<Row> run(Connection conn, String db, String table, String query, Function<ResultSet, List<RowElement>> converter, Function<ResultSetMetaData, Map<String, Integer>> indexMapper) {
		try(
			PreparedStatement statement = conn.prepareStatement(query);
			ResultSet results = statement.executeQuery();
		) {
			List<Row> rows = new ArrayList<>();
			Map<String, Integer> indices = indexMapper.apply(results.getMetaData());
			while(results.next()) {
				rows.add(new Row(db, table, converter.apply(results), indices));
			}
						
			return rows;
		} catch (SQLException e) {
			log.error("Could not execute query '{}'", query);
		}
		return Collections.emptyList();
	}
}
