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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.ingef.eva.async.AsyncDumpProcessor;
import de.ingef.eva.async.AsyncWriter;
import de.ingef.eva.configuration.Configuration;
import de.ingef.eva.configuration.ConfigurationReader;
import de.ingef.eva.configuration.DatabaseEntry;
import de.ingef.eva.configuration.DatabaseQueryConfiguration;
import de.ingef.eva.configuration.JsonConfigurationReader;
import de.ingef.eva.constant.Templates;
import de.ingef.eva.processor.Processor;
import de.ingef.eva.processor.RemovePattern;
import de.ingef.eva.utility.Dataset;
import de.ingef.eva.utility.Helper;

public class Main {
	public static void main(String[] args) {
		final Logger logger = LogManager.getRootLogger();

		//first argument given to jar is the configuration json file
		final String configFilePath = args[0];
		if(!configFilePath.isEmpty())
		{
			final ConfigurationReader configReader = new JsonConfigurationReader();
			final Configuration configuration = configReader.ReadConfiguration(configFilePath);
			
			if(args.length > 1)
			{
				if(args[1].equalsIgnoreCase("makejob"))
				{
					createFastExportJobs(configuration, logger);
					logger.info("Teradata FastExport job file created.");
				}
				else
				{
					logger.warn("Unknown command: {}.", args[1]);
				}
			}
			else
			{
				final ExecutorService threadPool = Executors.newFixedThreadPool(configuration.getThreadCount());

				try
				{
					final long start = System.nanoTime();
					//remove special characters like null or ack first
					Processor<String> removeControlSequences = new RemovePattern("[^\\p{Alnum};]");
					//then remove leading and trailing whitespaces
					Processor<String> removeBoundaryWhitespaces = new RemovePattern("^\\s+|\\s+$");
					Collection<Processor<String>> processors = new ArrayList<Processor<String>>();
					processors.add(removeControlSequences);
					processors.add(removeBoundaryWhitespaces);
					
					File[] filenames = new File(String.format("%s/.",configuration.getTempDirectory())).listFiles();
					List<Dataset> dumpFiles = Helper.findDatasets(filenames);
					for(Dataset ds : dumpFiles)
					{
						AsyncDumpProcessor dumpCleaner = new AsyncDumpProcessor(
															processors,
															ds,
															configuration.getOutDirectory(),
															String.format("%s.csv", ds.getName()),
															configuration.getFastExportConfiguration().getRowPrefix(),
															logger);
						threadPool.execute(dumpCleaner);
					}
					
					threadPool.shutdown();
					threadPool.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
					logger.info("Time taken: {} min.", Helper.NanoSecondsToMinutes(System.nanoTime() - start));
					logger.info("Done.");
				}
				catch (InterruptedException e) {
					logger.error("Thread interrupted: {}", e.getMessage());
				}
				finally
				{
					if(!threadPool.isTerminated())
						threadPool.shutdownNow();
				}
			}
		}
		else
		{
			logger.error("No config.json file given.");
		}
	}

	/**
	 * Creates a Teradata FastExport script for dumping data
	 * @param configuration
	 * @param logger
	 */
	private static void createFastExportJobs(Configuration configuration, Logger logger)
	{
		try (
				Connection connection = DriverManager.getConnection(
						configuration.createFullConnectionUrl(),
						configuration.getUsername(),
						configuration.getUserpassword()
						);
				Statement stm = connection.createStatement();
				BufferedWriter writer = new BufferedWriter(new FileWriter(configuration.getFastExportConfiguration().getJobFilename()))
				) {
			StringBuilder tasks = new StringBuilder();
			DatabaseQueryConfiguration dbConfig = configuration.getDatabaseQueryConfiguration();
			ExecutorService threadPool = Executors.newCachedThreadPool();
			for(DatabaseEntry dbEntry : dbConfig.getEntries())
			{
				String db = dbEntry.getName();
				StringBuilder header = new StringBuilder();
				for(String table : dbEntry.getTables())
				{
					ResultSet rs = stm.executeQuery(String.format(Templates.QUERY_COLUMNS, db, table));

					StringBuilder columnSelectBuilder = new StringBuilder();
					columnSelectBuilder.append(String.format("';%s'||", configuration.getFastExportConfiguration().getRowPrefix()));
					boolean hasYearColumn = false;
					while(rs.next())
					{
						String column = rs.getString(1).trim();
						header.append(column);
						header.append(";");
						//set only once
						hasYearColumn = (!hasYearColumn) ? column.equalsIgnoreCase("Bezugsjahr") : true;
						columnSelectBuilder.append(String.format(Templates.COLUMN_PROCESSING, column));
						columnSelectBuilder.append("||';'||");
					}
					rs.close();
					startHeaderWriterTask(configuration, logger, threadPool, db, header, table);
					int unusedSemicolonIndex = columnSelectBuilder.lastIndexOf(";");
					//remove trailing semicolon
					columnSelectBuilder = columnSelectBuilder.delete(unusedSemicolonIndex - 3, columnSelectBuilder.length());
					
					String task;
					if(hasYearColumn)
					{
						int[] years = Helper.extractYears(configuration.getDatabaseQueryConfiguration());
						StringBuilder subtasks = new StringBuilder();
						String select = columnSelectBuilder.toString();
						for(int year : years)
						{
							subtasks.append(
									String.format(Templates.TASK_FORMAT, configuration.getFastExportConfiguration().getSessions())
									.replace("$OUTFILE", String.format("%s/%s_%s.%d.csv", configuration.getTempDirectory(), db,table, year))
									.replace("$QUERY", String.format(
											Templates.RESTRICTED_QUERY_FORMAT,
											select, 
											db,
											table, "Bezugsjahr="+year))
									);
						}
						task = subtasks.toString();
					}
					else
					{
						task = String.format(Templates.TASK_FORMAT, configuration.getFastExportConfiguration().getSessions())
								.replace("$OUTFILE", String.format("%s/%s_%s.csv", configuration.getTempDirectory(), db, table))
								.replace("$QUERY", 
										String.format(Templates.QUERY_FORMAT, columnSelectBuilder.toString(), db, table)
										);
					}

					tasks.append(task);
				}
			}
			threadPool.shutdown();
			
			String job = String.format(Templates.JOB_FORMAT,
					configuration.getFastExportConfiguration().getLogDatabase(),
					configuration.getFastExportConfiguration().getLogTable(),
					configuration.getServer(),
					configuration.getUsername(), 
					configuration.getUserpassword(),
					tasks.toString(),
					configuration.getFastExportConfiguration().getPostDumpAction()
					);
			
			writer.write(job);
			threadPool.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS);

		} catch (SQLException e) {
			if(logger != null)
			{
				logger.error("Could not execute query.\nStackTrace: {}", e.getStackTrace());
			}
			else
			{
				e.printStackTrace();
			}
		} catch (IOException e1) {
			if(logger != null)
			{
				logger.error("Could not create job file.\nStackTrace: {}", e1.getStackTrace());
			}
			else
			{
				e1.printStackTrace();
			}
		} catch (InterruptedException e) {
			if(logger != null)
			{
				logger.error("Could not create header files.\nStackTrace: {}", e.getStackTrace());
			}
			else
			{
				e.printStackTrace();
			}
		}
	}

	private static void startHeaderWriterTask(Configuration configuration, Logger logger, ExecutorService threadPool,
			String db, StringBuilder header, String table) {
		//remove trailing semicolon
		header.deleteCharAt(header.length()-1);
		List<String> headerList = new ArrayList<String>(1);
		headerList.add(header.toString());
		threadPool.execute(new AsyncWriter(configuration.getTempDirectory()+"/"+db+"_"+table+".header.csv",
				headerList,
				logger));
	}
}
