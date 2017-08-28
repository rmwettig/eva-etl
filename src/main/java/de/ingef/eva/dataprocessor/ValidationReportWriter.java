package de.ingef.eva.dataprocessor;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

import de.ingef.eva.data.DataTable;
import de.ingef.eva.data.RowElement;
import de.ingef.eva.datasource.DataProcessor;
import de.ingef.eva.error.DataTableOperationException;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;

/**
 * Takes one or more report data tables and creates a single report for them
 * @author Martin.Wettig
 *
 */
@Log4j2
@RequiredArgsConstructor
public class ValidationReportWriter implements DataProcessor {
	private final String reportFileName;
	private final String outputDirectory;
	
	@Override
	public DataTable process(DataTable... dataTables) {
		try(BufferedWriter writer = new BufferedWriter(new FileWriter(outputDirectory + "/" + reportFileName));) {
			for(DataTable dt : dataTables) {
				if(!dt.open()) {
					log.error("Could not open report data '{}'", dt.getName());
					return null;
				}
				writer.write(String.format("# Row length check for %s\n", dt.getName()));
				writer.write("## Defective rows\n");
				while(dt.hasMoreRows()) {
					writer.write(convertToString(dt.getNextRow(true)));
					writer.write("\n");
				}
				writer.write("\n");
			}
		} catch(DataTableOperationException e) {
			log.error(e);
		} catch (IOException e) {
			log.error("Could not write report file '{}'.\n\t{}", reportFileName, e);
		} 
		
		return null;	
	}
	
	private String convertToString(List<RowElement> row) {
		String format = "File: %s\tRow: %s\tExpected: %s, was: %s\tContent: '%s'";
		return String.format(format, row.get(0).getContent(), row.get(1).getContent(), row.get(2).getContent(), row.get(3).getContent(), row.get(4).getContent());
	}
	
}
