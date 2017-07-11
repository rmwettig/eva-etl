package de.ingef.eva.datasource.file;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import de.ingef.eva.data.DataTable;
import de.ingef.eva.data.RowElement;
import de.ingef.eva.data.SimpleRowElement;
import de.ingef.eva.data.TeradataColumnType;
import de.ingef.eva.error.DataTableOperationException;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;

/**
 * Raw data table file that has no information about column data types.
 * @author Martin.Wettig
 *
 */
@Log4j2
@RequiredArgsConstructor
public class FileDataTable implements DataTable {

	protected final File file;
	@Getter
	protected final String delimiter;
	@Getter
	private final String name;
	private final List<RowElement> columnNames;
	private BufferedReader reader;
	
	@Override
	public List<RowElement> getColumnNames() throws DataTableOperationException {
		return columnNames;
	}

	/**
	 * Reads the next row from the underlying file. Rows that do not match the length of given header are dropped.
	 * @return column entries. Empty list if row is truncated.
	 */
	@Override
	public List<RowElement> getNextRow(boolean ignoreMalformedRows) throws DataTableOperationException {
		try {
			String line = prepareLine(reader.readLine());
			//in case of empty lines return no columns
			if(line.isEmpty())
				return new ArrayList<>();
			String[] columns = line.split(delimiter, -1);
			
			if(ignoreMalformedRows) {
				return createFilteredRow(columns);
			} else {
				return createUnfilteredRow(columns);
			}
		} catch (IOException e) {
			throw new DataTableOperationException("Could not read next line of data table '"+name+"'", e);
		} catch (DataTableOperationException e) {
			throw e;
		}
	}

	private List<RowElement> createFilteredRow(String[] columns) {
		List<RowElement> row = new ArrayList<>();
		//quick fix to handle columns which contain semicolons in content
		if(columns.length != columnNames.size()) {
			log.warn("File '{}': Cannot load line {} as it has an invalid column count. Expected {} columns but was {}", name, columns, columnNames.size(), columns.length);
			return row;
		}
		for(int i = 0; i < columnNames.size(); i++) {
			RowElement columnHeader = columnNames.get(i);
			//assign the data element to the corresponding column name and type
			row.add(new SimpleRowElement(columnHeader.getContent(), i, columnHeader.getType(), columns[i]));
		}
		return row;
	}

	private List<RowElement> createUnfilteredRow(String[] columns) {
		List<RowElement> row = new ArrayList<>(columns.length);
		for(int i = 0; i < columns.length; i++)
			row.add(new SimpleRowElement("", i, TeradataColumnType.UNKNOWN, columns[i]));
		return row;
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
	
	/**
	 * apply preprocessing to a raw line read from file. By default, no processing is done. 
	 * @param line raw line from file
	 * @return
	 */
	protected String prepareLine(String line) throws DataTableOperationException {
		return line;
	}
}
