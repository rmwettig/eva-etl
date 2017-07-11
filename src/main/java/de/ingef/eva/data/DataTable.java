package de.ingef.eva.data;

import java.util.List;

import de.ingef.eva.error.DataTableOperationException;

public interface DataTable {
	public List<RowElement> getColumnNames() throws DataTableOperationException;
	public List<RowElement> getNextRow(boolean ignoreMalformedRows) throws DataTableOperationException;
	public boolean hasMoreRows() throws DataTableOperationException;
	public boolean open() throws DataTableOperationException;
	public void close() throws DataTableOperationException;
	public String getName();
	public String getDelimiter();
}
