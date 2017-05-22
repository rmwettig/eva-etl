package de.ingef.eva.datasource.file;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import de.ingef.eva.datasource.DataTable;
import de.ingef.eva.error.DataTableOperationException;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class FileDataTable implements DataTable {

	private final File file;
	private final String delimiter;
	@Getter
	private final String name;
	private BufferedReader reader;
	private List<String> columnNames;
	
	@Override
	public List<String> getColumnNames() throws DataTableOperationException {
		return columnNames;
	}

	@Override
	public List<String> getColumnTypes() throws DataTableOperationException {
		return new ArrayList<String>();
	}

	@Override
	public String[] getNextRow() throws DataTableOperationException {
		try {
			return reader.readLine().split(delimiter, -1);
		} catch (IOException e) {
			throw new DataTableOperationException("Could not read next line of data table '"+name+"'", e);
		}
	}

	@Override
	public boolean hasMoreRows() throws DataTableOperationException {
		if(reader == null) return false;
		try {
			return reader.ready();
		} catch (IOException e) {
			throw new DataTableOperationException("Could not check for further rows in data table'"+name+"'", e);
		}
	}

	@Override
	public boolean open() throws DataTableOperationException {
		if(reader != null) return true;
		try {
			reader = new BufferedReader(new FileReader(file));
			return true;
		} catch (FileNotFoundException e) {
			throw new DataTableOperationException("Could not open data table file '"+name+"'", e);
		}
	}

	@Override
	public void close() throws DataTableOperationException {
		try {
			reader.close();
		} catch (IOException e) {
			throw new DataTableOperationException("Could not close data table file '"+name+"'", e);
		}
	}
}
