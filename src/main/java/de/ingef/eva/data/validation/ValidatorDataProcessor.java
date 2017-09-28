package de.ingef.eva.data.validation;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import de.ingef.eva.configuration.Configuration;
import de.ingef.eva.constant.OutputDirectory;
import de.ingef.eva.data.DataTable;
import de.ingef.eva.data.RowElement;
import de.ingef.eva.datasource.DataProcessor;
import de.ingef.eva.datasource.file.FileDataTable;
import de.ingef.eva.error.DataTableOperationException;
import de.ingef.eva.utility.CsvWriter;
import de.ingef.eva.utility.Helper;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;

/**
 * Ensures valid values in each column of the data table
 * 
 * @author Martin.Wettig
 *
 */
@Log4j2
@RequiredArgsConstructor
public class ValidatorDataProcessor implements DataProcessor {

	private final Configuration configuration;
	private final Collection<Rule> validators;

	@Override
	public DataTable process(DataTable... dataTables) {
		DataTable dt = dataTables[0];
		
		try {
			String cleanFolder = configuration.getOutputDirectory()+ "/"+ OutputDirectory.CLEAN;
			Helper.createFolders(cleanFolder);
			File cleanedFile = new File(cleanFolder +"/" + dt.getName() + ".csv");
			CsvWriter writer = new CsvWriter(cleanedFile);
			
			writer.open();
			dt.open();
			
			for(RowElement header : dt.getColumnNames())
				writer.addEntry(header.getContent());
			writer.writeLine();
			
			while(dt.hasMoreRows()) {
				List<RowElement> row = dt.getNextRow(true);
				//no elements present for writing
				if(row.size() == 0 || row.size() != dt.getColumnNames().size()) {
					log.warn("Cannot clean row in file '{}'. Expected length: {}, was: {}. Line: '{}'", dt.getName(), dt.getColumnNames().size(), row.size(), row.stream().map(e -> e.getContent()).collect(Collectors.joining(dt.getDelimiter())));	
					continue;
				}
				
				RowElement validatedElement = null;
				for(RowElement element : row) {
					validatedElement = element;
					for(Rule validator : validators) {
						validatedElement = validator.validate(validatedElement);
					}
					writer.addEntry(validatedElement.getContent());
				}
				writer.writeLine();
			}
			dt.close();
			writer.close();
			return new FileDataTable(cleanedFile, ";", dt.getName(), dt.getColumnNames());
		} catch(DataTableOperationException e) {
			log.error("Could not validate data table '{}'.\n\tReason: {}. StackTrace: ", dt.getName(), e.getMessage(), e);
		} catch (IOException e) {
			log.error("Could not create CSVPrinter '{}'.\n\tReason: {}. StackTrace: ", e.getMessage(), e);
		}
		
		return null;
	}

}
