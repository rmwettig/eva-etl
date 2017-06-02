package de.ingef.eva.datasource.inmemory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import de.ingef.eva.datasource.DataTable;
import de.ingef.eva.error.DataTableOperationException;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class InMemoryDataTable implements DataTable {

	@Getter
	private final String name;
	private final Iterator<String[]> rows;
	private final List<String> header;
	
	@Override
	public List<String> getColumnNames() throws DataTableOperationException {
		return header;
	}

	@Override
	public List<String> getColumnTypes() throws DataTableOperationException {
		return new ArrayList<String>();
	}

	@Override
	public String[] getNextRow() throws DataTableOperationException {
		return rows.next();
	}

	@Override
	public boolean hasMoreRows() throws DataTableOperationException {
		return rows.hasNext();
	}

	@Override
	public boolean open() throws DataTableOperationException {
		return true;
	}

	@Override
	public void close() throws DataTableOperationException {
	}
}
