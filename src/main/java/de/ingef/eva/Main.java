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
import de.ingef.eva.etl.Filter;
import de.ingef.eva.etl.Merger;
import de.ingef.eva.etl.Transformer;
import de.ingef.eva.etl.TransformerFactory;
import de.ingef.eva.mapping.ProcessPidDecode;
import de.ingef.eva.measures.cci.CalculateCharlsonScores;
import de.ingef.eva.measures.statistics.Statistics;
import de.ingef.eva.query.JsonQuerySource;
import de.ingef.eva.query.Query;
import de.ingef.eva.utility.Helper;
import de.ingef.eva.utility.IOManager;
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
			} else if(cmd.hasOption("hash")) {
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
		Stopwatch sw = new Stopwatch();
		sw.start();
		Configuration configuration = Configuration.loadFromJson(cmd.getOptionValue("merge"));
		new Merger().run(configuration, IOManager.of(configuration));
		sw.stop();
		log.info("Merging done in {}.", sw.createReadableDelta());
	}

	private static void charlsonscores(CommandLine cmd) throws JsonProcessingException, IOException {
		Configuration configuration = Configuration.loadFromJson(cmd.getOptionValue("charlsonscores"));
		exitIfInvalidCredentials(configuration);
		CalculateCharlsonScores.calculate(configuration);
	}

	private static void export(CommandLine cmd) throws JsonProcessingException, IOException {
		Stopwatch sw = new Stopwatch();
		sw.start();
		Configuration config = Configuration.loadFromJson(cmd.getOptionValue("dump"));
		exitIfInvalidCredentials(config);
		Collection<Query> queries = new JsonQuerySource(config).createQueries();
		List<Filter> filters = config.getFilters();
		filters.stream().forEach(filter -> filter.initialize(config));
		List<Transformer> transformers = new TransformerFactory().create(config, config.getTransformers());
		new ETLPipeline().run(config, queries, filters, transformers, IOManager.of(config));
		sw.stop();
		log.info("Dumping done in {}.", sw.createReadableDelta());
	}

	private static void exitIfInvalidCredentials(Configuration config) {
		if(!Helper.areCredentialsCorrect(config.getUser(), config.getPassword(), config.getFullConnectionUrl())) {
			System.err.println("Invalid credentials.\n\tURL: " + config.getFullConnectionUrl() + "\n" +	"\tUser: " + config.getUser());
			System.exit(-1);
		}
	}
	
	private static Options createCliOptions() {
		Options options = new Options();
		options.addOption(Option.builder("charlsonscores").hasArg().argName("config.json").desc("calculate Charlson scores").build());
		options.addOption(Option.builder("makedecode").hasArg().argName("config.json").desc("create PID mappings").build());
		options.addOption(Option.builder("stats").hasArg().argName("config.json").desc("create database content statistics").build());
		options.addOption(Option.builder("dump").hasArg().argName("config.json").desc("run FastExport scripts").build());
		options.addOption(Option.builder("merge").hasArg().argName("config.json").desc("merge clean data slices").build());
		options.addOption(Option.builder("hash").hasArg().argName("config.json").desc("creates a file that contains mappings from pid to hashes").build());
		
		return options;
	}
	
	private static void createPidHashes(CommandLine cmd) throws JsonProcessingException, IOException {
		Stopwatch sw = new Stopwatch();
		sw.start();
		Configuration config = Configuration.loadFromJson(cmd.getOptionValue("hash"));
		exitIfInvalidCredentials(config);
		config.getHashing().calculateHashes(config);
		sw.stop();
		log.info("Create hash mappings in {}.", sw.createReadableDelta());
	}
	
	private static void createPidMappings(Configuration configuration) {
		List<DecodingConfig> decodingConfigs = configuration.getDecode();
		for(DecodingConfig dc : decodingConfigs) {
			String h2iks = dc.getH2iks().stream().map(h -> "'" + h + "'").collect(Collectors.joining(", "));
			DataSource unfilteredPids = new SqlDataSource(String.format(Templates.Decoding.PID_DECODE_QUERY, h2iks, h2iks), dc.getName(), configuration);
			DataSource excludedPids = new SqlDataSource(String.format(Templates.Decoding.INVALID_PIDS_QUERY, h2iks, h2iks), dc.getName(), configuration);
			DataProcessor cleanPidProcessor = new ProcessPidDecode(IOManager.of(configuration));
			cleanPidProcessor.process(unfilteredPids.fetchData(), excludedPids.fetchData());
		}
		
	}
	
	private static void createDatabaseStatistics(Configuration configuration) {
		new Statistics().createStatistics(configuration, IOManager.of(configuration));
	}
}
