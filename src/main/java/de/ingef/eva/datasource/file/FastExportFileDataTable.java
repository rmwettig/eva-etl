package de.ingef.eva.datasource.file;

import java.io.File;
import java.util.List;

import de.ingef.eva.data.RowElement;
import de.ingef.eva.error.DataTableOperationException;

public class FastExportFileDataTable extends FileDataTable {
	private String rowStartSignal;
	
	public FastExportFileDataTable(File file, String delimiter, String name, List<RowElement> columnNames, String rowStartSignal) {
		super(file, delimiter, name, columnNames);
		this.rowStartSignal = rowStartSignal;
	}
	
	@Override
	protected String prepareLine(String line) throws DataTableOperationException {
		if(line.isEmpty() || rowStartSignal == null || rowStartSignal.isEmpty()) return line;
		try {
			return line.substring(line.indexOf(rowStartSignal) + rowStartSignal.length() + delimiter.length());
		} catch (IndexOutOfBoundsException e) {
			throw new DataTableOperationException("Row start signal'" + rowStartSignal + "' was not found in file '" + file.getName() +"'.", e);
		}
	}
}
