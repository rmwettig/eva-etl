package de.ingef.eva.dataprocessor;

import java.io.File;
import java.io.IOException;
import java.util.List;

import de.ingef.eva.data.DataTable;
import de.ingef.eva.data.RowElement;
import de.ingef.eva.datasource.DataProcessor;
import de.ingef.eva.datasource.file.FileDataTable;
import de.ingef.eva.error.DataTableOperationException;
import de.ingef.eva.utility.CsvWriter;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;

/**
 * Takes multiple data tables and combines them into a single result
 * @author Martin.Wettig
 *
 */
@Log4j2
@RequiredArgsConstructor
public class DataTableMergeProcessor implements DataProcessor {

	private final String outputDirectory;
	
	@Override
	public DataTable process(DataTable... dataTables) {
		//TODO change signature to avoid unnecessary array creation
		DataTable dataset = dataTables[0];
		File mergeFile = new File(outputDirectory + "/" + dataset.getName() + ".csv");
		try {
			CsvWriter writer = new CsvWriter(mergeFile);
			writer.open();
			if(!dataset.open())
				log.error("Could not open data set '{}'", dataset.getName());
			while(dataset.hasMoreRows()) {
				List<RowElement> row = dataset.getNextRow(false);
				if(row.isEmpty()) continue;
				for(RowElement e : row) {
					writer.addEntry(e.getContent());
				}
				writer.writeLine();
			}			
			dataset.close();
			writer.close();
			return new FileDataTable(mergeFile, writer.getDelimiter(), dataset.getName(), dataset.getColumnNames());
		} catch (DataTableOperationException e) {
			log.error("Could not process data set '{}'.\n\tReason:{}. StackTrace: ", dataset.getName(), e.getMessage(), e);
		} catch (IOException e1) {
			log.error("Could not create file '{}'.\n\tReason: {}\n\tStackTrace: ", mergeFile.getName(), e1.getMessage(), e1);
		}
		
		return null;
	}

}
