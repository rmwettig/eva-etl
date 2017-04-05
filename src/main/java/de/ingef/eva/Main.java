package de.ingef.eva;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;

import de.ingef.eva.async.AsyncDumpProcessor;
import de.ingef.eva.async.AsyncMapper;
import de.ingef.eva.async.AsyncWriter;
import de.ingef.eva.configuration.Configuration;
import de.ingef.eva.configuration.ConfigurationDatabaseHostLoader;
import de.ingef.eva.configuration.ConfigurationReader;
import de.ingef.eva.configuration.JsonConfigurationReader;
import de.ingef.eva.configuration.JsonInterpreter;
import de.ingef.eva.configuration.Mapping;
import de.ingef.eva.configuration.SchemaDatabaseHostLoader;
import de.ingef.eva.configuration.SqlJsonInterpreter;
import de.ingef.eva.configuration.Target;
import de.ingef.eva.constant.Templates;
import de.ingef.eva.database.Database;
import de.ingef.eva.database.DatabaseHost;
import de.ingef.eva.database.Table;
import de.ingef.eva.processor.Processor;
import de.ingef.eva.processor.ReplacePattern;
import de.ingef.eva.query.Query;
import de.ingef.eva.query.QueryCreator;
import de.ingef.eva.query.SimpleQueryCreator;
import de.ingef.eva.utility.Alias;
import de.ingef.eva.utility.Dataset;
import de.ingef.eva.utility.Helper;

public class Main {
	public static void main(String[] args) {
		final Logger logger = LogManager.getRootLogger();

		// first argument given to jar is the configuration json file
		final String configFilePath = args[0];
		if (!configFilePath.isEmpty()) {
			final ConfigurationReader configReader = new JsonConfigurationReader();
			final Configuration configuration = configReader.ReadConfiguration(configFilePath);

			if (args.length > 1) {
				if (args[1].equalsIgnoreCase("makejob")) {
					createFastExportJobs(configuration, logger);
					logger.info("Teradata FastExport job file created.");
				} else if (args[1].equalsIgnoreCase("fetchschema")) {
					DatabaseHost schema = new ConfigurationDatabaseHostLoader(logger).loadFromFile(configFilePath);
					createHeaderLookup(configuration, schema, logger);
					logger.info("Teradata column lookup created.");
				} else if (args[1].equalsIgnoreCase("map")) {
					mapFiles(logger, configuration);
				} else {
					logger.warn("Unknown command: {}.\nValid commands are:\n\t makejob\n\fetchschema\n\tmap", args[1]);
				}
			} else {
				cleanData(logger, configuration);
			}
		} else {
			logger.error("No config.json file given.");
		}
	}

	private static Collection<Processor<String>> createProcessors() {
		//remove special characters like null or ack first
		//but keep diacritics (ä, ö, ü, ß), § and € 
		Processor<String> removeControlSequences = new ReplacePattern("[^\\p{Alnum};.\u00e4\u00f6\u00fc\u00df\u00a7\u20ac-]", "");
		//then remove leading and trailing whitespaces
		Processor<String> removeBoundaryWhitespaces = new ReplacePattern("^\\s+|\\s+$", "");
		Collection<Processor<String>> processors = new ArrayList<Processor<String>>();
		processors.add(removeControlSequences);
		processors.add(removeBoundaryWhitespaces);
		
		return processors;
	}

	/**
	 * Creates a JSON with column names for defined tables
	 * 
	 * @param configuration
	 * @param logger
	 */
	private static void createHeaderLookup(Configuration configuration, DatabaseHost schema, Logger logger) {
		try (Connection connection = DriverManager.getConnection(configuration.createFullConnectionUrl(),
				configuration.getUsername(), configuration.getPassword());
				Statement stm = connection.createStatement();
				JsonGenerator jsonWriter = new JsonFactory()
						.createGenerator(new FileWriter(configuration.getSchemaFilePath()));) {

			jsonWriter.writeStartObject(); // json start
			for (Database dbEntry : schema.getAllDatabases()) {
				String db = dbEntry.getName();
				jsonWriter.writeObjectFieldStart(db); // db object
				for (Table table : dbEntry.getAllTables()) {
					jsonWriter.writeFieldName(table.getName()); // array name
					jsonWriter.writeStartArray(); // array bracket

					ResultSet rs = stm.executeQuery(String.format(Templates.QUERY_COLUMNS, db, table.getName()));

					while (rs.next()) {
						String column = rs.getString(1).trim();
						jsonWriter.writeString(column);
					}
					rs.close();
					jsonWriter.writeEndArray();// table
				}
				jsonWriter.writeEndObject();// db
			}
			jsonWriter.writeEndObject();// json
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (IOException e1) {
			e1.printStackTrace();
		}
	}

	/**
	 * Creates a Teradata FastExport script for dumping data
	 * 
	 * @param configuration
	 * @param logger
	 */
	private static void createFastExportJobs(Configuration configuration, Logger logger) {
		try (BufferedWriter writer = new BufferedWriter(
				new FileWriter(configuration.getFastExportConfiguration().getJobFilename()))) {
			StringBuilder tasks = new StringBuilder();
			ExecutorService threadPool = Executors.newCachedThreadPool();
			//schema is used to look up tables or columns in a table
			DatabaseHost schema = new SchemaDatabaseHostLoader().loadFromFile(configuration.getSchemaFilePath());
			QueryCreator queryCreator = new SimpleQueryCreator();
			queryCreator.setAliasFactory(new Alias(120));
			JsonInterpreter jsonInterpreter = new SqlJsonInterpreter(queryCreator, schema, logger);
			Collection<Query> jobs = jsonInterpreter.interpret(configuration.getDatabaseNode());
			
			Helper.createFolders(configuration.getTempDirectory());
			for (Query q : jobs) {
				tasks.append(
						String.format(Templates.TASK_FORMAT, configuration.getFastExportConfiguration().getSessions(),
								configuration.getTempDirectory() + "/" + q.getName() + ".csv", q.getQuery()));
				createHeaderWriterTask(configuration.getTempDirectory(), logger, q, threadPool );
			}

			threadPool.shutdown();

			String job = String.format(Templates.JOB_FORMAT,
					configuration.getFastExportConfiguration().getLogDatabase(),
					configuration.getFastExportConfiguration().getLogTable(), configuration.getServer(),
					configuration.getUsername(), configuration.getPassword(), tasks.toString(),
					configuration.getFastExportConfiguration().getPostDumpAction());

			writer.write(job);
			threadPool.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS);

		} catch (IOException e1) {
			if (logger != null) {
				logger.error("Could not create job file.\nStackTrace: {}", e1.getStackTrace());
			} else {
				e1.printStackTrace();
			}
		} catch (InterruptedException e) {
			if (logger != null) {
				logger.error("Could not create header files.\nStackTrace: {}", e.getStackTrace());
			} else {
				e.printStackTrace();
			}
		}
	}

	private static void createHeaderWriterTask(String directory, Logger logger, Query q, ExecutorService threadPool) {
		List<String> headerList = new ArrayList<String>(1);
		headerList.add(combineColumnHeaders(q.getSelectedColumns()));
		String prefix = q.getName().substring(0, q.getName().indexOf("."));
		String path = directory + "/" + prefix + ".header.csv";
		File f = new File(path);
		if(!f.exists())
			threadPool.execute(new AsyncWriter(path, headerList, logger));
	}

	private static String combineColumnHeaders(Collection<String> columns) {
		StringBuilder header = new StringBuilder();

		for (String s : columns) {
			header.append(s);
			header.append(";");
		}
		// remove trailing semicolon
		header.deleteCharAt(header.length() - 1);

		return header.toString();
	}

	private static void cleanData(Logger logger, Configuration configuration) {
		final ExecutorService threadPool = Executors.newFixedThreadPool(configuration.getThreadCount());

		try {
			final long start = System.nanoTime();
			Collection<Processor<String>> processors = createProcessors();
			File[] filenames = new File(String.format("%s/.", configuration.getTempDirectory())).listFiles();
			List<Dataset> dumpFiles = Helper.findDatasets(filenames);

			for (Dataset ds : dumpFiles) {
				Helper.createFolders(configuration.getOutDirectory());
				AsyncDumpProcessor dumpCleaner = new AsyncDumpProcessor(processors, ds, configuration.getOutDirectory(),
						String.format("%s.csv", ds.getName()),
						configuration.getFastExportConfiguration().getRowPrefix(), logger);
				threadPool.execute(dumpCleaner);
			}

			threadPool.shutdown();
			threadPool.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
			logger.info("Time taken: {} min.", Helper.NanoSecondsToMinutes(System.nanoTime() - start));
			logger.info("Done.");
		} catch (InterruptedException e) {
			logger.error("Thread interrupted: {}", e.getMessage());
		} finally {
			if (!threadPool.isTerminated())
				threadPool.shutdownNow();
		}
	}

	private static void mapFiles(Logger logger, Configuration configuration) {
		final ExecutorService threadPool = Executors.newFixedThreadPool(configuration.getThreadCount());
		final Collection<Mapping> mappings = configuration.getMappings();
		
		if (mappings.size() == 0) {
			logger.error("No mapping configuration found.");
			
			return;
		}
		for(Mapping m : mappings) {
			final String mapFile = m.getMappingFileName();
			final Map<String,String> egk2pid = Helper.createMappingFromFile(mapFile);
			
			if(egk2pid == null) {
				logger.error("Could not create mapping from file {}.", mapFile);
				
				return;
			}
			
			final String sourceKeyName = m.getSourceColumn();
			final String targetKeyName = m.getTargetColumn();
			for(Target t : m.getTargets()) {
				int columnIndex = Helper.findColumnIndexfromHeaderFile(t.getHeaderFile(), ",", sourceKeyName);
				threadPool.execute(new AsyncMapper(egk2pid, t.getDataFile(), columnIndex, targetKeyName));
			}
		}
		threadPool.shutdown();
		try {
			threadPool.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

}
