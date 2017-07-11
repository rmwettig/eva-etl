package de.ingef.eva.query;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;

import de.ingef.eva.configuration.Configuration;
import de.ingef.eva.configuration.FastExportConfiguration;
import de.ingef.eva.constant.OutputDirectory;
import de.ingef.eva.constant.Templates;
import de.ingef.eva.data.DataTable;
import de.ingef.eva.database.Column;
import de.ingef.eva.error.QueryExecutionException;
import de.ingef.eva.utility.Helper;
import de.ingef.eva.utility.SimpleFileWriter;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;

@RequiredArgsConstructor
@Log4j2
public class FastExportJobWriter implements QueryExecutor<Query> {	
	private final Configuration configuration;
	@Override
	public DataTable execute(Query query) throws QueryExecutionException {
		String jobFolder = configuration.getOutputDirectory() + "/" + OutputDirectory.FEXP_JOBS;
		String headerFolder = configuration.getOutputDirectory() + "/" + OutputDirectory.HEADERS;
		String rawFolder = configuration.getOutputDirectory() + "/" + OutputDirectory.RAW;
		Helper.createFolders(jobFolder);
		Helper.createFolders(rawFolder);
		Helper.createFolders(headerFolder);
		
		String baseName = query.getName();
		String jobFile = jobFolder + "/" + baseName + ".fx";
		FastExportConfiguration feConfig = configuration.getFastExportConfiguration();
		try {
			String content = String.format(
					Templates.TASK_FORMAT,
					feConfig.getSessions(),
					rawFolder + "/" + baseName + ".csv", query.getQuery()
			);
			//TODO remove system command from fe job (or make it optional)
			//TODO perhaps create a query mapper that transforms a query object into a string representation
			//TODO parallelize writing
			String logSuffix = Integer.toHexString(query.getName().hashCode());
			content = String.format(
					Templates.JOB_FORMAT,
					feConfig.getLogDatabase(), feConfig.getLogTable() + logSuffix,
					configuration.getHost(), configuration.getUser(), configuration.getPassword(),
					content,
					""
				);
			SimpleFileWriter.writeToFile(jobFile, content);
		} catch (IOException e) {
			throw new QueryExecutionException(String.format("Could not create file '%s'", jobFile), e);
		}
		
		//in case that the current query represents a year slice
		//if a query db_table.yyyy is given the resulting header file 
		//is named db_table.header
		int endIdx = baseName.indexOf('.');
		String headerFile = endIdx >= 0 ? baseName.substring(0, endIdx) + ".header" : baseName + ".header";
		try {
			createHeaderFile(headerFolder + "/" + headerFile, query);
		} catch (IOException e) {
			throw new QueryExecutionException(String.format("Could not create file '%s'", headerFile), e);
		}
		
		return null;
	}
	
	private void createHeaderFile(String headerFilePath, Query query) throws IOException {
		File header = new File(headerFilePath);
		if(header.exists())
		{
			log.info("Header '{}' exists already.", headerFilePath);
			return;
		}
		
		JsonGenerator writer = new JsonFactory().createGenerator(new FileWriter(header));
		writer.writeStartObject();
		writer.writeFieldName("columns");
		writer.writeStartArray();
		for(Column column : query.getSelectedColumns()) {
			writer.writeStartObject();
			writer.writeStringField("column", column.getName());
			writer.writeStringField("type", column.getType().toString());
			writer.writeEndObject();
		}
		writer.writeEndArray();
		writer.writeEndObject();
		writer.close();
	}

}
