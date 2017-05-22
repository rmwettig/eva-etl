package de.ingef.eva.datasource;

import java.util.List;

import de.ingef.eva.error.DataTableOperationException;

public interface DataTable {
	public List<String> getColumnNames() throws DataTableOperationException;
	public List<String> getColumnTypes() throws DataTableOperationException;
	public String[] getNextRow() throws DataTableOperationException;
	public boolean hasMoreRows() throws DataTableOperationException;
	public boolean open() throws DataTableOperationException;
	public void close() throws DataTableOperationException;
	public String getName();
}
