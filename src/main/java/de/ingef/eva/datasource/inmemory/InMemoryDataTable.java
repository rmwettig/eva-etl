package de.ingef.eva.datasource.inmemory;

import java.util.Iterator;
import java.util.List;

import de.ingef.eva.data.DataTable;
import de.ingef.eva.data.RowElement;
import de.ingef.eva.error.DataTableOperationException;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class InMemoryDataTable implements DataTable {

	@Getter
	private String delimiter = "";
	@Getter
	private final String name;
	private final Iterator<List<RowElement>> rows;
	private final List<RowElement> header;
	
	@Override
	public List<RowElement> getColumnNames() throws DataTableOperationException {
		return header;
	}

	@Override
	public List<RowElement> getNextRow(boolean ignoreMalformedRows) throws DataTableOperationException {
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
