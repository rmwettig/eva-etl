package de.ingef.eva.data.validation;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import de.ingef.eva.data.DataTable;
import de.ingef.eva.data.RowElement;
import de.ingef.eva.data.SimpleRowElement;
import de.ingef.eva.data.TeradataColumnType;
import de.ingef.eva.datasource.DataProcessor;
import de.ingef.eva.datasource.inmemory.InMemoryDataTable;
import de.ingef.eva.error.DataTableOperationException;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;

@Log4j2
@RequiredArgsConstructor
public class RowLengthValidator implements DataProcessor {
	private final String dbName;
	private final Map<String,Integer> table2ColumnCount;
	
	@Override
	public DataTable process(DataTable... dataTables) {
		List<List<RowElement>> reportElements = new ArrayList<>();
		try {
			for(DataTable dt : dataTables) {
				if(dt == null) continue;
				dt.open();
				//TODO find a generalized way to handle file parts and merged files. Currently, they have the form <DB>_<TABLE>.year.csv
				String commonName = dt.getName().contains(".") ? dt.getName().substring(0, dt.getName().indexOf(".")) : dt.getName();
				int expectedColumnCount = findDatabaseTable(commonName);
				if(expectedColumnCount == -1) {
					log.warn("No column count found for file '{}'", dt.getName());
					continue; 
				}
				
				int rowIndex = 0;
				//iterate over rows and check for length
				while(dt.hasMoreRows()) {
					List<RowElement> row = dt.getNextRow(false);
					if(row == null) {
						log.error("Line {} is null.", rowIndex++);
						continue;
					}

					int actualColumnCount = row.size();
					//if length is not right save row information
					if(expectedColumnCount != actualColumnCount) {
						List<RowElement> entry = Arrays.asList(
								new SimpleRowElement("File", 0, TeradataColumnType.CHARACTER, dt.getName()),
								new SimpleRowElement("RowIndex", 1, TeradataColumnType.CHARACTER, Integer.toString(rowIndex)),
								new SimpleRowElement("ExpectedColumnCount", 2, TeradataColumnType.CHARACTER, Integer.toString(expectedColumnCount)),
								new SimpleRowElement("ActualColumnCount", 2, TeradataColumnType.CHARACTER, Integer.toString(actualColumnCount)),
								new SimpleRowElement("RowContent", 2, TeradataColumnType.CHARACTER, row.stream().map(e -> e.getContent()).collect(Collectors.joining(dt.getDelimiter())))
						);
						reportElements.add(entry);
					}
					rowIndex++;
				}
				dt.close();
			}
			List<RowElement> header = Arrays.asList(
					new SimpleRowElement("File", 0, TeradataColumnType.CHARACTER, "File"),
					new SimpleRowElement("RowIndex", 1, TeradataColumnType.CHARACTER, "RowIndex"),
					new SimpleRowElement("ExpectedColumnCount", 2, TeradataColumnType.CHARACTER, "ExpectedColumnCount"),
					new SimpleRowElement("ActualColumnCount", 3, TeradataColumnType.CHARACTER, "ActualColumnCount"),
					new SimpleRowElement("RowContent", 4, TeradataColumnType.CHARACTER, "RowContent")
			);
			
			return new InMemoryDataTable(dbName, reportElements.iterator(), header);
		} catch (DataTableOperationException e) {
			log.error("{}", e);
		}
		return null;
	}
	
	private int findDatabaseTable(String name) {
		if(table2ColumnCount.containsKey(name))
			return table2ColumnCount.get(name);
		return -1;
	}

}
