package de.ingef.eva.datasource.sql;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import de.ingef.eva.data.DataTable;
import de.ingef.eva.data.RowElement;
import de.ingef.eva.data.SimpleRowElement;
import de.ingef.eva.data.TeradataColumnType;
import de.ingef.eva.error.DataTableOperationException;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class SqlDataTable implements DataTable {

	private final ResultSet resultSet;
	private final ResultSetMetaData metaData;
	@Getter
	private final String name;
	@Getter
	private String delimiter = "";
	
	@Override
	public List<RowElement> getColumnNames() throws DataTableOperationException {
		List<RowElement> columns;
		try {
			columns = new ArrayList<>(metaData.getColumnCount());
			for(int i = 1; i <= metaData.getColumnCount(); i++) {
				String label = metaData.getColumnLabel(i);
				columns.add(createSimpleRowElement(i, label));
			}
		} catch (SQLException e) {
			throw new DataTableOperationException("Could not retrieve column names.", e);
		}

		return columns;
	}

	@Override
	public List<RowElement> getNextRow(boolean ignoreMalformedRows) throws DataTableOperationException {
		List<RowElement> row = new ArrayList<>();
		try {
			int columnCount = metaData.getColumnCount();
			for(int i = 1; i <= columnCount; i++) {
				String content = resultSet.getString(i) == null ? "" : resultSet.getString(i).trim();
				row.add(createSimpleRowElement(i, content));
			}
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
	
	/**
	 * creates 0-based indexed RowElement
	 * @param index column index
	 * @param content column content
	 * @return
	 * @throws SQLException
	 */
	private SimpleRowElement createSimpleRowElement(int index, String content) throws SQLException {
		String label = metaData.getColumnLabel(index);
		TeradataColumnType type = TeradataColumnType.mapCodeToName(metaData.getColumnTypeName(index));
		return new SimpleRowElement(label, index-1, type, content);
	}
}
