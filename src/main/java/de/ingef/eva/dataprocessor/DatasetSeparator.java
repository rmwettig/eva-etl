package de.ingef.eva.dataprocessor;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import de.ingef.eva.constant.OutputDirectory;
import de.ingef.eva.data.DataTable;
import de.ingef.eva.data.RowElement;
import de.ingef.eva.datasource.DataProcessor;
import de.ingef.eva.error.DataTableOperationException;
import de.ingef.eva.utility.CsvWriter;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;

/**
 * Separates ADB data based on h2ik
 * @author martin.wettig
 *
 */
@Log4j2
@RequiredArgsConstructor
public class DatasetSeparator implements DataProcessor {

	private final SeparationMapping h2ik2dataset;
	private final String rootDirectory;
	
	@Override
	public DataTable process(DataTable... dataTables) {
		DataTable dt = dataTables[0];
		try {
			if(!dt.open())
				throw new DataTableOperationException("Could not open data table '" + dt.getName() + "'", null);
			int h2ikColumnIndex = findH2ikColumnIndex(dt.getColumnNames());
			if(h2ikColumnIndex == -1)
				throw new DataTableOperationException("File '" + dt.getName() + "' has no column 'h2ik'", null);
			Map<String,CsvWriter> outputs = createFileOutputs(h2ik2dataset.getDatasetNames(), dt.getName() + ".csv");
			int lineNumber = 1;
			writeHeader(dt.getColumnNames(), outputs.values());
			while(dt.hasMoreRows()) {
				List<RowElement> row = dt.getNextRow(true);
				String h2ik = row.get(h2ikColumnIndex).getContent();
				if(h2ik == null || h2ik.isEmpty()) {
					log.warn("File: '{}' contained no h2ik in line {}", dt.getName(), lineNumber++);
					continue;
				}
				//in case the column name is read
				if(h2ik.equalsIgnoreCase("h2ik")) continue;
				//write to corresponding file in corresponding directory
				String dataset = h2ik2dataset.findDatasetName(h2ik);
				if(dataset == null) {
					log.warn("File: '{}' has no dataset found for H2IK: '{}' (line: {})", h2ik, lineNumber++);
					continue;
				}
				CsvWriter writer = outputs.get(dataset);
				row.stream().forEach(e -> writer.addEntry(e.getContent()));
				writer.writeLine();
				++lineNumber;
			}

			outputs.forEach((dataset, writer) -> {
				try {
					writer.close();
				} catch (IOException e) {
					log.error("Could not close file '{}' for dataset '{}'", dt.getName(), dataset);
					return;
				}
			});

		} catch (DataTableOperationException e) {
			log.error("Could not execut operation on data table '{}'.", dt.getName(), e);
		} catch (IOException e) {
			log.error("Could not create outputs: ", e);
		}
		
		return null;
	}

	private void writeHeader(List<RowElement> columnNames, Collection<CsvWriter> outputs) throws IOException {
		for(CsvWriter writer : outputs) {
			columnNames.stream().forEach(e -> writer.addEntry(e.getContent()));
			writer.writeLine();
		}
	}

	/**
	 * Searches header for h2ik column
	 * @param columnNames
	 * @return -1 if h2ik column was not found
	 */
	private int findH2ikColumnIndex(List<RowElement> columnNames) {
		for (int i = 0; i < columnNames.size(); i++) {
			if(columnNames.get(i).getContent().equalsIgnoreCase("h2ik"))
				return i;
		}
		
		return -1;		
	}
	
	private Map<String,CsvWriter> createFileOutputs(Collection<String> datasets, String fileName) throws IOException {
		Map<String,CsvWriter> outputs = new HashMap<>(datasets.size());
		for(String dataset : datasets) {
			CsvWriter writer = new CsvWriter(Paths.get(rootDirectory, OutputDirectory.PRODUCTION, dataset, fileName).toFile());
			writer.open();
			outputs.put(dataset, writer);
		}
		return outputs;
	}
}
