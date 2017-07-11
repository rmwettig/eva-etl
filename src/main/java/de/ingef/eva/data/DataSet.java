package de.ingef.eva.data;

import java.util.List;

import de.ingef.eva.error.DataTableOperationException;
import lombok.RequiredArgsConstructor;

/**
 * Represents a group of data tables that constitute a full data set
 * @author Martin.Wettig
 *
 */
@RequiredArgsConstructor
public class DataSet implements DataTable {
	
	private final String name;
	private final List<DataTable> subsets;
	private final boolean removeSubsetHeader;
	private int currentSubsetIndex = 0;
	
	@Override
	public String getDelimiter() {
		return subsets.get(currentSubsetIndex).getDelimiter();
	}
	
	@Override
	public List<RowElement> getColumnNames() throws DataTableOperationException {
		return subsets.get(currentSubsetIndex).getColumnNames();
	}

	@Override
	public List<RowElement> getNextRow(boolean ignoreMalformedRows) throws DataTableOperationException {
		return subsets.get(currentSubsetIndex).getNextRow(ignoreMalformedRows);
	}

	@Override
	public boolean hasMoreRows() throws DataTableOperationException {
		//more rows if current subset has more rows
		if(subsets.get(currentSubsetIndex).hasMoreRows()) return true;
		//more rows if current subset has no more rows but currentSubsetIndex can be incremented
		if(!subsets.get(currentSubsetIndex).hasMoreRows() &&
				currentSubsetIndex < subsets.size() - 1) {
			subsets.get(currentSubsetIndex++).close();
			DataTable nextTable = subsets.get(currentSubsetIndex);
			nextTable.open();

			//if header should be removed and not returned as a row
			if(removeSubsetHeader) {
				boolean isEmptyFile = false;
				do {
					//no non-empty file was found
					if(currentSubsetIndex == subsets.size() - 1) return false;
					
					if(nextTable.hasMoreRows()) {
						//remove header
						nextTable.getNextRow(true);
						//check if table still has rows after header removal
						isEmptyFile = !nextTable.hasMoreRows();
						//if empty go to next data table
						if(isEmptyFile) {
							nextTable.close();
							nextTable = subsets.get(++currentSubsetIndex);
							nextTable.open();
						}
					}
				} while(isEmptyFile);
			}
			return true;
		}
		
		//no more rows otherwise
		return false;
	}

	@Override
	public boolean open() throws DataTableOperationException {
		return subsets.get(currentSubsetIndex).open();
	}

	@Override
	public void close() throws DataTableOperationException {
		subsets.get(currentSubsetIndex).close();
	}

	@Override
	public String getName() {
		return name;
	}

}
