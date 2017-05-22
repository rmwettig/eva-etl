package de.ingef.eva.datasource.sql;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import de.ingef.eva.datasource.DataTable;
import de.ingef.eva.error.DataTableOperationException;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class SqlDataTable implements DataTable {

	private final ResultSet resultSet;
	private final ResultSetMetaData metaData;
	@Getter
	private final String name;
	
	@Override
	public List<String> getColumnNames() throws DataTableOperationException {
		List<String> columns;
		try {
			columns = new ArrayList<>(metaData.getColumnCount());
			for(int i = 1; i <= metaData.getColumnCount(); i++)
				columns.add(metaData.getColumnLabel(i));
		} catch (SQLException e) {
			throw new DataTableOperationException("Could not retrieve column names.", e);
		}

		return columns;
	}

	@Override
	public List<String> getColumnTypes() {
		List<String> columns = new ArrayList<>();
		
		return columns;
	}

	@Override
	public String[] getNextRow() throws DataTableOperationException {
		String[] row;
		try {
			int columnCount = metaData.getColumnCount();
			row = new String[columnCount];
			for(int i = 1; i <= columnCount; i++)
				row[i-1] = resultSet.getString(i).trim();
		} catch (SQLException e) {
			throw new DataTableOperationException("Could not retrieve next row.", e);
		}
		
		return row;
	}

	@Override
	public boolean hasMoreRows() throws DataTableOperationException {
		try {
			if(resultSet.isClosed()) return false;
			resultSet.next();
			return !resultSet.isAfterLast();
		} catch (SQLException e) {
			throw new DataTableOperationException("Could not check if more rows are available", e);
		}
	}

	@Override
	public void close() throws DataTableOperationException {
		try {
			resultSet.close();
			resultSet.getStatement().close();
			resultSet.getStatement().getConnection().close();
		} catch (SQLException e) {
			throw new DataTableOperationException("Could not close data table.", e);
		}
	}

	@Override
	public boolean open() throws DataTableOperationException {
		return true;
	}

}
