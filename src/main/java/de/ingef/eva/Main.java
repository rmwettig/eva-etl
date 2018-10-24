package de.ingef.eva;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import com.fasterxml.jackson.core.JsonProcessingException;

import de.ingef.eva.configuration.Configuration;
import de.ingef.eva.configuration.decoding.DecodingConfig;
import de.ingef.eva.constant.Templates;
import de.ingef.eva.datasource.DataProcessor;
import de.ingef.eva.datasource.DataSource;
import de.ingef.eva.datasource.sql.SqlDataSource;
import de.ingef.eva.etl.ETLPipeline;
import de.ingef.eva.etl.filters.Filter;
import de.ingef.eva.etl.Merger;
import de.ingef.eva.etl.transformers.Transformer;
import de.ingef.eva.etl.transformers.TransformerFactory;
import de.ingef.eva.mapping.ProcessPidDecode;
import de.ingef.eva.measures.cci.CalculateCharlsonScores;
import de.ingef.eva.measures.statistics.Statistics;
import de.ingef.eva.query.JsonQuerySource;
import de.ingef.eva.query.Query;
import de.ingef.eva.services.ConnectionFactory;
import de.ingef.eva.services.TaskRunner;
import de.ingef.eva.services.TeradataConnectionFactory;
import de.ingef.eva.utility.Helper;
import de.ingef.eva.utility.io.IOManager;
import de.ingef.eva.utility.Stopwatch;
import lombok.extern.log4j.Log4j2;

@Log4j2
public class Main {

	private static final String EXPORT_COMMAND = "export";
	private static final String STATS_COMMAND = "stats";
	private static final String MAKEDECODE_COMMAND = "makedecode";
	private static final String MERGE_COMMAND = "merge";
	private static final String HASH_COMMAND = "hash";

	public static void main(String[] args) {
		Options options = createCliOptions();
		CommandLineParser parser = new DefaultParser();

		try {
			CommandLine cmd = parser.parse(options, args);
			if(cmd.hasOption(EXPORT_COMMAND)) {
				export(cmd);
			} else if(cmd.hasOption("charlsonscores")) {
				charlsonscores(cmd);
			} else if (cmd.hasOption(MAKEDECODE_COMMAND)) {
				Configuration configuration = Configuration.loadFromJson(cmd.getOptionValue(MAKEDECODE_COMMAND));
				exitIfInvalidCredentials(configuration);
				createPidMappings(configuration);
			} else if(cmd.hasOption(STATS_COMMAND)) {
				createDatabaseStatistics(Configuration.loadFromJson(cmd.getOptionValue(STATS_COMMAND)));
			} else if(cmd.hasOption(MERGE_COMMAND)) {
				merge(cmd);
			} else if(cmd.hasOption(HASH_COMMAND)) {
				createPidHashes(cmd);
			} else
				new HelpFormatter().printHelp("java -jar eva-data.jar", options);
		} catch (ParseException | IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			log.error("Cleaning was interrupted.\n\tReason: {}. StackTrace:", e.getMessage(), e);
		}
	}

	private static void merge(CommandLine cmd) throws JsonProcessingException, IOException, InterruptedException {
		log.info("Starting merge");
		Stopwatch sw = new Stopwatch();
		sw.start();
		Configuration configuration = Configuration.loadFromJson(cmd.getOptionValue(MERGE_COMMAND));
		new Merger().run(configuration, IOManager.of(configuration));
		sw.stop();
		log.info("Merge done in {}", sw.createReadableDelta());
	}

	private static void charlsonscores(CommandLine cmd) throws JsonProcessingException, IOException {
		Configuration configuration = Configuration.loadFromJson(cmd.getOptionValue("charlsonscores"));
		exitIfInvalidCredentials(configuration);
		CalculateCharlsonScores.calculate(configuration);
	}

	private static void export(CommandLine cmd) throws JsonProcessingException, IOException {
		log.info("Starting export");
		Stopwatch sw = new Stopwatch();
		sw.start();
		Configuration config = Configuration.loadFromJson(cmd.getOptionValue(EXPORT_COMMAND));
		TaskRunner taskRunner = new TaskRunner(config.getThreadCount());
		ConnectionFactory connectionFactory = new TeradataConnectionFactory(config.getUser(), config.getPassword(), config.getFullConnectionUrl());
		exitIfInvalidCredentials(config);
		Collection<Query> queries = new JsonQuerySource(config).createQueries();
		log.info("Setting up filters");
		List<Filter> filters = config.getFilters();
		filters.stream().forEach(filter -> filter.initialize(config));
		log.info("Setting up transformers");
		List<Transformer> transformers = new TransformerFactory().create(config, config.getTransformers());
		new ETLPipeline().run(config, queries, filters, transformers, IOManager.of(config), taskRunner, connectionFactory);
		sw.stop();
		log.info("Export done in {}", sw.createReadableDelta());
	}

	private static void exitIfInvalidCredentials(Configuration config) {
		if(!Helper.areCredentialsCorrect(config.getUser(), config.getPassword(), config.getFullConnectionUrl())) {
			System.err.println("Invalid credentials.\n\tURL: " + config.getFullConnectionUrl() + "\n" +	"\tUser: " + config.getUser());
			System.exit(-1);
		}
	}
	
	private static Options createCliOptions() {
		Options options = new Options();
		//disabled because broken
		//options.addOption(Option.builder("charlsonscores").hasArg().argName("config.json").desc("calculate Charlson scores").build());
		options.addOption(Option.builder(MAKEDECODE_COMMAND).hasArg().argName("config.json").desc("create PID mappings").build());
		options.addOption(Option.builder(STATS_COMMAND).hasArg().argName("config.json").desc("create database content statistics").build());
		options.addOption(Option.builder(EXPORT_COMMAND).hasArg().argName("config.json").desc("exports specified data from Teradata").build());
		options.addOption(Option.builder(MERGE_COMMAND).hasArg().argName("config.json").desc("merge clean data slices").build());
		options.addOption(Option.builder(HASH_COMMAND).hasArg().argName("config.json").desc("creates a file that contains mappings from pid to hashes").build());
		
		return options;
	}
	
	private static void createPidHashes(CommandLine cmd) throws JsonProcessingException, IOException {
		log.info("Starting hash calculation");
		Stopwatch sw = new Stopwatch();
		sw.start();
		Configuration config = Configuration.loadFromJson(cmd.getOptionValue(HASH_COMMAND));
		exitIfInvalidCredentials(config);
		ConnectionFactory connectionFactory = new TeradataConnectionFactory(config.getUser(), config.getPassword(), config.getFullConnectionUrl());
		config.getHashing().calculateHashes(config, new TaskRunner(config.getThreadCount()), connectionFactory);
		sw.stop();
		log.info("Created hash mappings in {}", sw.createReadableDelta());
	}
	
	private static void createPidMappings(Configuration configuration) {
		Stopwatch sw = new Stopwatch();
		log.info("Starting pid decode creation");
		sw.start();
		List<DecodingConfig> decodingConfigs = configuration.getDecode();
		for(DecodingConfig dc : decodingConfigs) {
			log.info("Processing {}", dc.getName());
			String h2iks = dc.getH2iks().stream().map(h -> "'" + h + "'").collect(Collectors.joining(", "));
			DataSource unfilteredPids = new SqlDataSource(String.format(Templates.Decoding.PID_DECODE_QUERY, h2iks, h2iks), dc.getName(), configuration);
			DataSource excludedPids = new SqlDataSource(String.format(Templates.Decoding.INVALID_PIDS_QUERY, h2iks, h2iks), dc.getName(), configuration);
			DataProcessor cleanPidProcessor = new ProcessPidDecode(IOManager.of(configuration));
			cleanPidProcessor.process(unfilteredPids.fetchData(), excludedPids.fetchData());
			log.info("{} completed", dc.getName());
		}
		sw.stop();
		log.info("Created pid decodings in {}", sw.createReadableDelta());
	}
	
	private static void createDatabaseStatistics(Configuration configuration) {
		log.info("Starting report creation");
		Stopwatch sw = new Stopwatch();
		sw.start();
		new Statistics().createStatistics(configuration, IOManager.of(configuration));
		sw.stop();
		log.info("Reports created in {}", sw.createReadableDelta());

	}
}
