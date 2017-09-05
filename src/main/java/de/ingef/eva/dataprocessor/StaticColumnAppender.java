package de.ingef.eva.dataprocessor;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import de.ingef.eva.configuration.append.AppendOrder;
import de.ingef.eva.constant.OutputDirectory;
import de.ingef.eva.data.DataTable;
import de.ingef.eva.data.RowElement;
import de.ingef.eva.datasource.DataProcessor;
import de.ingef.eva.error.DataTableOperationException;
import de.ingef.eva.utility.CsvWriter;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;

@Log4j2
@RequiredArgsConstructor
public class StaticColumnAppender implements DataProcessor {

	private final String newColumnName;
	private final String constantValue;
	private final String datasetName;
	private final AppendOrder order;
	private final String outputFolder;
	
	@Override
	public DataTable process(DataTable... dataTables) {
		DataTable dt = dataTables[0];
		Path p = Paths.get(outputFolder, OutputDirectory.PRODUCTION, datasetName);
		if(!Files.exists(p)) {
			try {
				Files.createDirectories(p);
			} catch (IOException e) {
				log.error("Could not create directory '{}'. {}", p.toString(), e);
			}
		}
		
		final CsvWriter writer = new CsvWriter(p.resolve(dt.getName() + ".csv").toFile());
		try {
			dt.open();
			writer.open();
			switch(order) {
				case FIRST:
					appendToBeginning(dt, writer);
					break;
				case LAST:
					appendToEnd(dt, writer);
					break;
			}
		} catch (DataTableOperationException e) {
			log.error("Could not execute operation on data table '{}'. {}", dt.getName(), e);
		} catch (IOException e1) {
			log.error("Could not create file writer for data table '{}'. {}", dt.getName(), e1);
		} finally {
			try {
				dt.close();
			} catch (DataTableOperationException e) {
				log.error("Could not close data table '{}'", dt.getName());
			}
			
			try {
				if(writer != null) writer.close();
			} catch (IOException e) {
				log.error("Could not close StaticColumnAppender writer for '{}'", dt.getName());
			}
		}
		return null;
	}

	private void appendToBeginning(DataTable dt, CsvWriter writer) throws DataTableOperationException, IOException {
		List<RowElement> header = dt.getColumnNames();
		writer.addEntry(newColumnName);
		header.stream().forEachOrdered(e -> writer.addEntry(e.getContent()));
		writer.writeLine();
		
		//remove header
		if(dt.hasMoreRows())
			dt.getNextRow(true);
		
		while(dt.hasMoreRows()) {
			writer.addEntry(constantValue);
			List<RowElement> row = dt.getNextRow(true);
			row.stream().forEachOrdered(e -> writer.addEntry(e.getContent()));
			writer.writeLine();
		}
	}

	private void appendToEnd(DataTable dt, CsvWriter writer) throws DataTableOperationException, IOException {
		List<RowElement> header = dt.getColumnNames();
		header.stream().forEachOrdered(e -> writer.addEntry(e.getContent()));
		writer.addEntry(newColumnName);
		writer.writeLine();
		
		while(dt.hasMoreRows()) {
			List<RowElement> row = dt.getNextRow(true);
			row.stream().forEachOrdered(e -> writer.addEntry(e.getContent()));
			writer.addEntry(constantValue);
			writer.writeLine();
		}
	}

}
