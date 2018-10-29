package de.ingef.eva.tasks;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.Spliterators;
import java.util.function.Function;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import de.ingef.eva.error.TaskExecutionException;
import de.ingef.eva.etl.Row;
import de.ingef.eva.query.Query;
import de.ingef.eva.services.ConnectionFactory;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;

@Log4j2
public class SqlTask extends Task<Stream<Row>> {

	private final Query query;
	private final ConnectionFactory connectionFactory;
	private final Function<ResultSet, Row> resultConverter;
	
	public SqlTask(String name, Query query, ConnectionFactory connectionFactory, Function<ResultSet, Row> resultConverter) {
		super(name, query.getDescription());
		this.query = query;
		this.connectionFactory = connectionFactory;
		this.resultConverter = resultConverter;
	}

	@Override
	public Stream<Row> execute() {
		Connection conn = null;
		PreparedStatement statement = null;
		ResultSet result = null;
		try {
			//jdbc resources will be closed after the stream has been consumed
			conn = connectionFactory.createConnection();
			statement = conn.prepareStatement(query.getQuery());
			result = statement.executeQuery();
			return StreamSupport.stream(
					Spliterators.spliteratorUnknownSize(
							new ResultSetIterator(conn, statement, result, createConverterWithQueryData(query, resultConverter)),
							0
					),
					false);
		} catch (SQLException e) {
			log.error("{}, Error: {}", createErrorMessage(), e);
				try {
					if(statement != null)
						statement.close();
					if(conn != null)
						conn.close();
				} catch (SQLException e1) {
					log.error("Could not close resources. {}", e1);
				}
			throw new TaskExecutionException(createErrorMessage(), e);
		}
	}
	
	private String createErrorMessage() {
		return new StringBuilder()
			.append("Failed to execute query '")
			.append(query.getQuery())
			.append("'")
			.append(", Description: ")
			.append(query.getDescription())
			.toString();
	}
	
	/**
	 * wraps the original result set converter with table and db information of the query
	 * @param query
	 * @param converter
	 * @return
	 */
	private Function<ResultSet, Row> createConverterWithQueryData(Query query, Function<ResultSet, Row> converter) {
		return (resultSet) -> {
			Row row = converter.apply(resultSet);
			row.setDb(query.getDbName());
			row.setTable(query.getTableName());
			return row;
		};
	}
	
	@Log4j2
	@RequiredArgsConstructor
	private static class ResultSetIterator implements Iterator<Row> {

		private final Connection connection;
		private final PreparedStatement statement;
		private final ResultSet resultSet;
		private final Function<ResultSet, Row> rowConverter;
		
		@Override
		public boolean hasNext() {
			boolean hasNext = false;
			try {
				hasNext = resultSet.next();
			} catch (SQLException e) {
				log.error("Could not move to next row. {}", e);
				close();
			}
			if(!hasNext)
				close();
			return hasNext;
		}

		@Override
		public Row next() {
			return rowConverter.apply(resultSet);
		}
		
		private void close() {
			try {
				resultSet.close();
				statement.close();
				if(connection.isValid(5))
					connection.close();
			} catch(SQLException e) {
				log.error("Could not close resources in iterator. {}", e);
			}
		}
		
	}
}
