package de.ingef.eva;

import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.AbstractMap.SimpleEntry;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;

import de.ingef.eva.async.AsyncMapper;
import de.ingef.eva.configuration.Configuration;
import de.ingef.eva.configuration.ConfigurationDatabaseHostLoader;
import de.ingef.eva.configuration.Mapping;
import de.ingef.eva.configuration.SchemaDatabaseHostLoader;
import de.ingef.eva.configuration.Target;
import de.ingef.eva.configuration.append.AppendConfiguration;
import de.ingef.eva.constant.OutputDirectory;
import de.ingef.eva.constant.Templates;
import de.ingef.eva.data.DataSet;
import de.ingef.eva.data.DataTable;
import de.ingef.eva.data.TeradataColumnType;
import de.ingef.eva.data.validation.RowLengthValidator;
import de.ingef.eva.data.validation.ValidatorDataProcessor;
import de.ingef.eva.database.Column;
import de.ingef.eva.database.Database;
import de.ingef.eva.database.DatabaseHost;
import de.ingef.eva.database.Table;
import de.ingef.eva.dataprocessor.DataTableMergeProcessor;
import de.ingef.eva.dataprocessor.DatasetSeparator;
import de.ingef.eva.dataprocessor.DetailStatisticsDataProcessor;
import de.ingef.eva.dataprocessor.DynamicColumnAppender;
import de.ingef.eva.dataprocessor.HTMLTableWriter;
import de.ingef.eva.dataprocessor.SeparationMapping;
import de.ingef.eva.dataprocessor.StaticColumnAppender;
import de.ingef.eva.dataprocessor.StatisticsDataProcessor;
import de.ingef.eva.dataprocessor.ValidationReportWriter;
import de.ingef.eva.datasource.DataProcessor;
import de.ingef.eva.datasource.DataSource;
import de.ingef.eva.datasource.sql.SqlDataSource;
import de.ingef.eva.error.InvalidConfigurationException;
import de.ingef.eva.error.QueryExecutionException;
import de.ingef.eva.mapping.ProcessPidDecode;
import de.ingef.eva.measures.CalculateCharlsonScores;
import de.ingef.eva.query.FastExportJobWriter;
import de.ingef.eva.query.JdbcQueryExecutor;
import de.ingef.eva.query.JsonQuerySource;
import de.ingef.eva.query.Query;
import de.ingef.eva.query.QueryExecutor;
import de.ingef.eva.query.QueryJob;
import de.ingef.eva.query.QuerySource;
import de.ingef.eva.utility.Helper;
import de.ingef.eva.utility.Stopwatch;
import lombok.extern.log4j.Log4j2;

@Log4j2
public class Main {
	public static void main(String[] args) {
		Options options = createCliOptions();
		CommandLineParser parser = new DefaultParser();
		try {
			CommandLine cmd = parser.parse(options, args);
			if(cmd.hasOption("makejob")) {
				Configuration config = Configuration.loadFromJson(cmd.getOptionValue("makejob"));
				exitIfInvalidCredentials(config);
				QuerySource qs = new JsonQuerySource(config);
				Collection<Query> queries = qs.createQueries();
				QueryExecutor<Query> executor = new FastExportJobWriter(config);
				for(Query query : queries) {
					executor.execute(query);
				}
				log.info("Teradata FastExport job file created.");
			} else if(cmd.hasOption("dump")) {
				Stopwatch sw = new Stopwatch();
				sw.start();
				Configuration config = Configuration.loadFromJson(cmd.getOptionValue("dump"));
				exitIfInvalidCredentials(config);
				QuerySource qs = new JsonQuerySource(config);
				Collection<Query> queries = qs.createQueries();
				//export processes must not survive main thread
				ExecutorService threadPool = Executors.newFixedThreadPool(
						config.getThreadCount(),
						new ThreadFactory() {
							public Thread newThread(Runnable r) {
								Thread t = Executors.defaultThreadFactory().newThread(r);
								t.setDaemon(true);
								return t;
							}
						}
					);
				CountDownLatch cdl = new CountDownLatch(queries.size());
				for(Query q : queries) {
					CompletableFuture.supplyAsync(() -> {
						try {
							new JdbcQueryExecutor(config, cdl).execute(q);
						} catch (QueryExecutionException e) {
							throw new RuntimeException("Could not execute query '"+ q.getName() +"'. Error: "+ e.getMessage() + ". Cause: " + e.getCause().getMessage());
						}
						return null;
					}, threadPool)
					.exceptionally(e -> {
						log.error("{}", e);
						return null;
					});
					
				}
				cdl.await();
				sw.stop();
				log.info("Dumping done in {}.", sw.createReadableDelta());
				
			} else if(cmd.hasOption("fetchschema")) {
				String path = cmd.getOptionValue("fetchschema");
				DatabaseHost schema = new ConfigurationDatabaseHostLoader().loadFromFile(path);
				Configuration configuration = Configuration.loadFromJson(path);
				createHeaderLookup(configuration, schema);
				log.info("Teradata column lookup created.");
			} else if(cmd.hasOption("map")) {
				Configuration configuration = Configuration.loadFromJson(cmd.getOptionValue("map"));
				mapFiles(configuration);
			} else if(cmd.hasOption("charlsonscores")) {
				Configuration configuration = Configuration.loadFromJson(cmd.getOptionValue("charlsonscores"));
				CalculateCharlsonScores.calculate(configuration, null);
			} else if(cmd.hasOption("clean")) {
				Stopwatch sw = new Stopwatch();
				sw.start();
				Configuration configuration = Configuration.loadFromJson(cmd.getOptionValue("clean"));
				Map<String,List<Column>> table2header = Helper.parseTableHeaders(configuration.getOutputDirectory() + "/" + OutputDirectory.HEADERS);
				Collection<DataTable> tables = Helper.loadDataTablesFromDirectory(
						configuration.getOutputDirectory() + "/" + OutputDirectory.RAW,
						".csv",
						table2header,
						configuration.getFastExportConfiguration().getRowPrefix(),
						configuration.getFastExportConfiguration().getRawColumnDelimiter(), ""
					);
				//TODO harmonize config general rules and column/table specific-rules
				ExecutorService threadPool = Executors.newFixedThreadPool(configuration.getThreadCount());
				for(DataTable table : tables) {
					threadPool.execute(new Runnable() {
						@Override
						public void run() {
							new ValidatorDataProcessor(configuration, configuration.getRules()).process(table);	
						}
					}); 
				}
				threadPool.shutdown();
				threadPool.awaitTermination(14, TimeUnit.DAYS);
				sw.stop();
				log.info("Cleaning done in {}.", sw.createReadableDelta());
			} else if (cmd.hasOption("makedecode")) {
				Configuration configuration = Configuration.loadFromJson(cmd.getOptionValue("makedecode"));
				exitIfInvalidCredentials(configuration);
				createPidMappings(configuration);
			} else if(cmd.hasOption("stats")) {
				createDatabaseStatistics(Configuration.loadFromJson(cmd.getOptionValue("stats")));
			} else if(cmd.hasOption("merge")) {
				Stopwatch sw = new Stopwatch();
				sw.start();
				Configuration configuration = Configuration.loadFromJson(cmd.getOptionValue("merge"));
				List<DataSet> datasets = Helper.findDatasets(configuration.getOutputDirectory() + "/" + OutputDirectory.CLEAN);
				ExecutorService threadPool = Executors.newFixedThreadPool(configuration.getThreadCount());
				String mergeDirectory = configuration.getOutputDirectory() + "/" + OutputDirectory.MERGED;
				Helper.createFolders(mergeDirectory);
				for(DataSet ds : datasets) {
					CompletableFuture
							.supplyAsync(
									() -> {
										log.info("Merging '{}'", ds.getName());
										DataTable dt = new DataTableMergeProcessor(mergeDirectory).process(ds);
										log.info("'{}' done.", dt.getName());
										return dt;
									},
									threadPool
							);
				}
				threadPool.shutdown();
				threadPool.awaitTermination(14, TimeUnit.DAYS);
				sw.stop();
				log.info("Merging done in {}.", sw.createReadableDelta());
			} else if(cmd.hasOption("validate")) {
				String[] validationOptions = cmd.getOptionValues("validate");
				Configuration configuration = Configuration.loadFromJson(validationOptions[0]);
				DatabaseHost schema = new SchemaDatabaseHostLoader().loadFromFile(configuration.getSchemaFile());
				//get data tables from different directories
				String rawDelimiter = configuration.getFastExportConfiguration().getRawColumnDelimiter();
				String[] dataDirectory = new String[] {
						OutputDirectory.RAW,
						OutputDirectory.CLEAN,
						OutputDirectory.PRODUCTION
				};
				Map<String,String> directory2delimiter = Collections.unmodifiableMap(
						Stream.of(
								new SimpleEntry<>(OutputDirectory.RAW, rawDelimiter),
								new SimpleEntry<>(OutputDirectory.CLEAN, ";"),
								new SimpleEntry<>(OutputDirectory.PRODUCTION, ";")
						).collect(Collectors.toMap(e ->  e.getKey(), e -> e.getValue()))
				);
				DataTable[] reportData = new DataTable[dataDirectory.length];
				for (int optionIndex = 1; optionIndex < validationOptions.length; optionIndex++) {
					String dbName = validationOptions[optionIndex];
					Map<String,Integer> table2ColumnCount = Helper.countColumnsInHeaderFiles("./" + dbName.toLowerCase() + "-column-count.csv");
					for(int i = 0; i < dataDirectory.length; i++) {
						String dataSource = dataDirectory[i];
						Collection<DataTable> dataTables = Helper.loadDataTablesFromDirectory(
								configuration.getOutputDirectory() + "/" + dataDirectory[i],
								"csv",
								Helper.parseTableHeaders(configuration.getOutputDirectory() + "/" + OutputDirectory.HEADERS),
								configuration.getFastExportConfiguration().getRowPrefix(),
								directory2delimiter.get(dataSource), dbName
						);
						Collection<Table> tables = schema.findDatabaseByName("ACC_" + dbName).getAllTables();
						//pass on to row length validator
						DataTable[] data = new DataTable[table2ColumnCount.size()];
						reportData[i] = new RowLengthValidator(dataSource.toUpperCase(), table2ColumnCount).process(dataTables.toArray(data));
					}
					//pass on to validation report writer
					new ValidationReportWriter(dbName + "-report.txt", configuration.getOutputDirectory()).process(reportData);
				}
			} else if(cmd.hasOption("separate")) {
				Configuration config = Configuration.loadFromJson(cmd.getOptionValue("separate"));
				SeparationMapping mapping = config.getDatasetMembership();
				for(String dataset : mapping.getDatasetNames()) {
					Helper.createFolders(Paths.get(config.getOutputDirectory(), OutputDirectory.PRODUCTION, dataset).toString());
				}
				Map<String,List<Column>> headers = Helper.parseTableHeaders(Paths.get(config.getOutputDirectory(), OutputDirectory.HEADERS).toString());
				Collection<DataTable> dataTables = Helper.loadDataTablesFromDirectory(Paths.get(config.getOutputDirectory(), OutputDirectory.MERGED).toString(), "csv", headers, "", ";", "ADB");
				ExecutorService threadPool = Helper.createThreadPool(config.getThreadCount(), false);
				for(DataTable dataTable : dataTables) {
					CompletableFuture.supplyAsync(
							() -> {
								log.info("Separating file '{}'", dataTable.getName());
								new DatasetSeparator(mapping, config.getOutputDirectory()).process(dataTable);
								log.info("'{}' done.", dataTable.getName());
								return null;
							},
							threadPool
						);
				}
				threadPool.shutdown();
				threadPool.awaitTermination(3, TimeUnit.DAYS);
			} else if(cmd.hasOption("append")) {
				Configuration config = Configuration.loadFromJson(cmd.getOptionValue("append"));
				List<AppendConfiguration> appendConf = config.getAppenderConfiguration();
				ExecutorService threadPool = Executors.newFixedThreadPool(config.getThreadCount());
				Map<String,List<Column>> headers = Helper.parseTableHeaders(config.getOutputDirectory() + "/" + OutputDirectory.HEADERS);
				Collection<DataTable> tables = null;
				for(int i = 0; i < appendConf.size(); i++) {
					AppendConfiguration append = appendConf.get(i);
					//check mapping mode
					switch (append.getMode()) {
						case STATIC:
							if(append.getTargetColumn() == null || append.getTargetColumn().isEmpty()) {
								log.error("Invalid 'targetColumn' value in appendConfig[{}]", i);
								break;
							}
							if(append.getValue() == null || append.getValue().isEmpty()) {
								log.error("Invalid 'value' in appendConfig[{}]", i);
								break;
							}
							tables = Helper.loadDataTablesFromDirectory(append.getTargets(), "csv", headers, "", ";", append.getMatch());
							for(DataTable dt : tables) {
								threadPool.execute(new Runnable() {
									@Override
									public void run() {
										new StaticColumnAppender(append.getTargetColumn(), append.getValue(), append.getMatch(), append.getOrder(), config.getOutputDirectory()).process(dt);	
									}
								});
							}
							break;
						case DYNAMIC:
							if(append.getKeyColumn() == null || append.getKeyColumn().isEmpty()) {
								log.error("Invalid 'keyColumn' value in appendConfig[{}]", i);
								break;
							}
			
							tables = Helper.loadDataTablesFromDirectory(append.getTargets(), "csv", headers, "", ";", append.getMatch());
							for(DataTable dt : tables) {
								DataTable extraColumns = Helper.loadExternalDataTable(Paths.get(append.getSource()));
								threadPool.execute(new Runnable() {
									@Override
									public void run() {
										new DynamicColumnAppender(append.getKeyColumn(), append.getMatch(), config.getOutputDirectory()).process(dt, extraColumns);
									}
								});
							}
							break;

						default:
							log.warn("Could not process append mode '{}'", append.getMode());
							break;
					}
				}
				threadPool.shutdown();
				threadPool.awaitTermination(3, TimeUnit.DAYS);
			}
			else
				new HelpFormatter().printHelp("java -jar eva-data.jar", options);
		} catch (ParseException | IOException | InvalidConfigurationException e) {
			e.printStackTrace();
		} catch (QueryExecutionException e) {
			log.error("Query execution failed: {}. StackTrace: ", e.getMessage(), e);
		} catch (InterruptedException e) {
			log.error("Cleaning was interrupted.\n\tReason: {}. StackTrace:", e.getMessage(), e);
		}
	}

	private static void exitIfInvalidCredentials(Configuration config) {
		if(!Helper.areCredentialsCorrect(config.getUser(), config.getPassword(), config.getFullConnectionUrl())) {
			System.err.println("Invalid credentials.\n\tURL: " + config.getFullConnectionUrl() + "\n" +	"\tUser: " + config.getUser());
			System.exit(-1);
		}
	}

	/**
	 * Creates a JSON with column names for defined tables
	 * 
	 * @param configuration
	 * @param logger
	 */
	private static void createHeaderLookup(Configuration configuration, DatabaseHost schema) {
		try (Connection connection = DriverManager.getConnection(
				configuration.getConnectionUrl()+ "/" +configuration.getConnectionParameters(),
				configuration.getUser(),
				configuration.getPassword()
				);
				Statement stm = connection.createStatement();
				JsonGenerator jsonWriter = new JsonFactory()
						.createGenerator(new FileWriter(configuration.getSchemaFile()));
				) {

			jsonWriter.writeStartObject(); // json start
			for (Database dbEntry : schema.getAllDatabases()) {
				String db = dbEntry.getName();
				jsonWriter.writeObjectFieldStart(db); // db object
				for (Table table : dbEntry.getAllTables()) {
					jsonWriter.writeFieldName(table.getName()); // array name
					jsonWriter.writeStartArray(); // array bracket

					ResultSet rs = stm.executeQuery(String.format(Templates.QUERY_COLUMNS, db, table.getName()));

					while (rs.next()) {
						jsonWriter.writeStartObject();
						jsonWriter.writeFieldName("column");
						jsonWriter.writeString(rs.getString(1).trim());
						jsonWriter.writeFieldName("type");
						String code = rs.getString(2).trim();
						TeradataColumnType type = TeradataColumnType.mapCodeToName(code); 
						jsonWriter.writeString(type != TeradataColumnType.UNKNOWN ? type.getLabel() : type.getLabel()+"("+code+")");
						jsonWriter.writeEndObject();
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


	private static void mapFiles(Configuration configuration) {
		final ExecutorService threadPool = Executors.newFixedThreadPool(configuration.getThreadCount());
		final Collection<Mapping> mappings = configuration.getMappings();
		
		if (mappings.size() == 0) {
			log.error("No mapping configuration found.");
			
			return;
		}
		for(Mapping m : mappings) {
			final String mapFile = m.getMappingFileName();
			final Map<String,String> egk2pid = Helper.createMappingFromFile(mapFile);
			
			if(egk2pid == null) {
				log.error("Could not create mapping from file {}.", mapFile);
				
				return;
			}
			
			final String sourceKeyName = m.getSourceColumn();
			final String targetKeyName = m.getTargetColumn();
			for(Target t : m.getTargets()) {
				int columnIndex = Helper.findColumnIndexfromHeaderFile(t.getHeaderFile(), ";", sourceKeyName);
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
	
	private static Options createCliOptions() {
		Options options = new Options();
		options.addOption(Option.builder("makejob").hasArg().argName("config.json").desc("create FastExport scripts").build());
		options.addOption(Option.builder("fetchschema").hasArg().argName("config.json").desc("create the database schema as a file").build());
		options.addOption(Option.builder("map").hasArg().argName("config.json").desc("map files to different ids").build());
		options.addOption(Option.builder("charlsonscores").hasArg().argName("config.json").desc("calculate Charlson scores").build());
		options.addOption(Option.builder("clean").hasArg().argName("config.json").desc("post-process dumped data").build());
		options.addOption(Option.builder("makedecode").hasArg().argName("config.json").desc("create PID mappings").build());
		options.addOption(Option.builder("stats").hasArg().argName("config.json").desc("create database content statistics").build());
		options.addOption(Option.builder("dump").hasArg().argName("config.json").desc("run FastExport scripts").build());
		options.addOption(Option.builder("merge").hasArg().argName("config.json").desc("merge clean data slices").build());
		options.addOption(Option.builder("validate").hasArgs().argName("config.json> ADB FDB").desc("performs row length validation of generated files. Datasets can be specified.").build());
		options.addOption(Option.builder("separate").hasArg().argName("config.json").desc("creates distinct ADB datasets").build());
		options.addOption(Option.builder("append").hasArg().argName("config.json").desc("adds a column to files matching the pattern").build());
		return options;
	}
	
	private static void createPidMappings(Configuration configuration) {
		Map<String,List<String>> name2h2ik = configuration.getNameToH2ik();
		for(String name : name2h2ik.keySet()) {
			String h2iks = name2h2ik.get(name).stream().map(h -> "'" + h + "'").collect(Collectors.joining(", "));
			DataSource unfilteredPids = new SqlDataSource(String.format(Templates.Decoding.PID_DECODE_QUERY, h2iks, h2iks), name, configuration);
			DataSource excludedPids = new SqlDataSource(String.format(Templates.Decoding.INVALID_PIDS_QUERY, h2iks, h2iks), name, configuration);
			DataProcessor cleanPidProcessor = new ProcessPidDecode(configuration);
			cleanPidProcessor.process(unfilteredPids.fetchData(), excludedPids.fetchData());
		}
		
	}
	
	private static void createDatabaseStatistics(Configuration configuration) {
		String[] tables = new String[]{"AU_Fall", "KH_Fall", "HeMi_EVO", "HiMi_EVO", "Arzt_Fall", "AM_EVO"};
		Map<String,List<String>> healthInsurances = new HashMap<>(2);
		healthInsurances.put("Bosch", Arrays.asList("108036123"));
		healthInsurances.put("Salzgitter", Arrays.asList("101922757", "101931440", "102137985"));
		for(String hi : healthInsurances.keySet()) {
			int i = 0;
			DataTable overview;
			//+1 because of detail stats
			DataTable[] stats = new DataTable[tables.length + 1];
			String iks = Helper.joinIks(healthInsurances.get(hi));
			for(String table : tables) {
				String query;
				switch(table) {
				case "AU_Fall":
					query = Templates.Statistics.ADB_STATISTICS_FOR_AU_FALL.replace("${tableSuffix}", table);
					break;
				case "KH_Fall":
					query = Templates.Statistics.ADB_STATISTICS_FOR_KH_FALL.replace("${tableSuffix}", table);
					break;
				case "HeMi_EVO":
				case "HiMi_EVO":
					query = Templates.Statistics.ADB_STATISTICS_FOR_HEMI_HIMI.replace("${tableSuffix}", table);
					break;
				case "Arzt_Fall":
					query = Templates.Statistics.ADB_STATISTICS_FOR_ARZT_FALL.replace("${tableSuffix}", table);
					break;
				case "AM_EVO":
					query = Templates.Statistics.ADB_STATISTICS_FOR_AM_EVO.replace("${tableSuffix}", table);
					break;
				default:
					continue;
				}
				query = query.replace("${h2ik}", iks);
				//TODO consolidate api. statsDataProcessor can take multiple data sources but htmlWriter processes only oneq
				DataTable dataTable = new SqlDataSource(query, table, configuration).fetchData();
				stats[i++] = new StatisticsDataProcessor().process(dataTable);
			}
			stats[i] = new DetailStatisticsDataProcessor().process(
					new SqlDataSource(Templates.Statistics.ADB_OUTPATIENT_DATA_BY_KV_QUERY.replaceAll("\\$\\{h2ik\\}", iks), "ArztFall_details", configuration).fetchData()
			);
			new HTMLTableWriter(configuration.getOutputDirectory(), hi + "_stats.html", "Datenstand der Datenbereiche zum elektronischen Datenaustausch der GKV").process(stats);			
		}
	}
}
