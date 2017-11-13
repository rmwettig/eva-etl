package de.ingef.eva;

import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;

import de.ingef.eva.configuration.Configuration;
import de.ingef.eva.configuration.ConfigurationDatabaseHostLoader;
import de.ingef.eva.configuration.decoding.DecodingConfig;
import de.ingef.eva.constant.Templates;
import de.ingef.eva.data.DataTable;
import de.ingef.eva.data.TeradataColumnType;
import de.ingef.eva.database.Database;
import de.ingef.eva.database.DatabaseHost;
import de.ingef.eva.database.Table;
import de.ingef.eva.dataprocessor.DetailStatisticsDataProcessor;
import de.ingef.eva.dataprocessor.HTMLTableWriter;
import de.ingef.eva.dataprocessor.StatisticsDataProcessor;
import de.ingef.eva.datasource.DataProcessor;
import de.ingef.eva.datasource.DataSource;
import de.ingef.eva.datasource.sql.SqlDataSource;
import de.ingef.eva.error.InvalidConfigurationException;
import de.ingef.eva.etl.ETLPipeline;
import de.ingef.eva.etl.Filter;
import de.ingef.eva.etl.FilterFactory;
import de.ingef.eva.etl.Merger;
import de.ingef.eva.etl.Transformer;
import de.ingef.eva.etl.TransformerFactory;
import de.ingef.eva.mapping.ProcessPidDecode;
import de.ingef.eva.measures.CalculateCharlsonScores;
import de.ingef.eva.query.JsonQuerySource;
import de.ingef.eva.query.Query;
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
			if(cmd.hasOption("dump")) {
				export(cmd);
			} else if(cmd.hasOption("fetchschema")) {
				fetchschema(cmd);
			} else if(cmd.hasOption("charlsonscores")) {
				charlsonscores(cmd);
			} else if (cmd.hasOption("makedecode")) {
				Configuration configuration = Configuration.loadFromJson(cmd.getOptionValue("makedecode"));
				exitIfInvalidCredentials(configuration);
				createPidMappings(configuration);
			} else if(cmd.hasOption("stats")) {
				createDatabaseStatistics(Configuration.loadFromJson(cmd.getOptionValue("stats")));
			} else if(cmd.hasOption("merge")) {
				merge(cmd);
			} else
				new HelpFormatter().printHelp("java -jar eva-data.jar", options);
		} catch (ParseException | IOException | InvalidConfigurationException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			log.error("Cleaning was interrupted.\n\tReason: {}. StackTrace:", e.getMessage(), e);
		}
	}

	private static void merge(CommandLine cmd)
			throws JsonProcessingException, IOException, InvalidConfigurationException, InterruptedException {
		Stopwatch sw = new Stopwatch();
		sw.start();
		Configuration configuration = Configuration.loadFromJson(cmd.getOptionValue("merge"));
		new Merger().run(configuration);
		sw.stop();
		log.info("Merging done in {}.", sw.createReadableDelta());
	}

	private static void charlsonscores(CommandLine cmd)
			throws JsonProcessingException, IOException, InvalidConfigurationException {
		Configuration configuration = Configuration.loadFromJson(cmd.getOptionValue("charlsonscores"));
		CalculateCharlsonScores.calculate(configuration, null);
	}

	private static void fetchschema(CommandLine cmd)
			throws JsonProcessingException, IOException, InvalidConfigurationException {
		String path = cmd.getOptionValue("fetchschema");
		DatabaseHost schema = new ConfigurationDatabaseHostLoader().loadFromFile(path);
		Configuration configuration = Configuration.loadFromJson(path);
		createHeaderLookup(configuration, schema);
		log.info("Teradata column lookup created.");
	}

	private static void export(CommandLine cmd)
			throws JsonProcessingException, IOException, InvalidConfigurationException {
		Stopwatch sw = new Stopwatch();
		sw.start();
		Configuration config = Configuration.loadFromJson(cmd.getOptionValue("dump"));
		exitIfInvalidCredentials(config);
		QuerySource qs = new JsonQuerySource(config);
		Collection<Query> queries = qs.createQueries();
		List<Filter> filters = new FilterFactory().create(config.getFilterConfiguration());
		List<Transformer> transformers = new TransformerFactory().create(config.getAppenderConfiguration());
		new ETLPipeline().run(config, queries, filters, transformers);
		sw.stop();
		log.info("Dumping done in {}.", sw.createReadableDelta());
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
				configuration.getFullConnectionUrl(),
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
			log.error("Could not open connection to '{}'. ", configuration.getFullConnectionUrl(), e);
		} catch (IOException e1) {
			e1.printStackTrace();
		}
	}
	
	private static Options createCliOptions() {
		Options options = new Options();
		options.addOption(Option.builder("fetchschema").hasArg().argName("config.json").desc("create the database schema as a file").build());
		options.addOption(Option.builder("map").hasArg().argName("config.json").desc("map files to different ids").build());
		options.addOption(Option.builder("charlsonscores").hasArg().argName("config.json").desc("calculate Charlson scores").build());
		options.addOption(Option.builder("makedecode").hasArg().argName("config.json").desc("create PID mappings").build());
		options.addOption(Option.builder("stats").hasArg().argName("config.json").desc("create database content statistics").build());
		options.addOption(Option.builder("dump").hasArg().argName("config.json").desc("run FastExport scripts").build());
		options.addOption(Option.builder("merge").hasArg().argName("config.json").desc("merge clean data slices").build());
		options.addOption(Option.builder("validate").hasArgs().argName("config.json> ADB FDB").desc("performs row length validation of generated files. Datasets can be specified.").build());
		options.addOption(Option.builder("separate").hasArg().argName("config.json").desc("creates distinct ADB datasets").build());
		return options;
	}
	
	private static void createPidMappings(Configuration configuration) {
		List<DecodingConfig> decodingConfigs = configuration.getDecode();
		for(DecodingConfig dc : decodingConfigs) {
			String h2iks = dc.getH2iks().stream().map(h -> "'" + h + "'").collect(Collectors.joining(", "));
			DataSource unfilteredPids = new SqlDataSource(String.format(Templates.Decoding.PID_DECODE_QUERY, h2iks, h2iks), dc.getName(), configuration);
			DataSource excludedPids = new SqlDataSource(String.format(Templates.Decoding.INVALID_PIDS_QUERY, h2iks, h2iks), dc.getName(), configuration);
			DataProcessor cleanPidProcessor = new ProcessPidDecode(configuration);
			cleanPidProcessor.process(unfilteredPids.fetchData(), excludedPids.fetchData());
		}
		
	}
	
	private static void createDatabaseStatistics(Configuration configuration) {
		createADBStatistics(configuration);
	}

	private static void createADBStatistics(Configuration configuration) {
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
