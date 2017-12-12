package de.ingef.eva.configuration;

import java.io.File;
import java.io.IOException;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import de.ingef.eva.configuration.append.AppendConfiguration;
import de.ingef.eva.configuration.cci.CCIConfig;
import de.ingef.eva.configuration.decoding.DecodingConfig;
import de.ingef.eva.configuration.export.ExportConfig;
import de.ingef.eva.configuration.statistics.StatisticConfig;
import de.ingef.eva.etl.FilterConfig;
import lombok.Getter;

/**
 * ETL configuration file
 * @author Martin.Wettig
 *
 */
@Getter
public class Configuration {
	/**
	 * Teradata host ip
	 */
	private String host;
	/**
	 * JDBC connection url
	 */
	private String url;
	/**
	 * Teradata connection parameters
	 */
	private String parameters;
	/**
	 * Teradata user name
	 */
	private String user;
	/**
	 * Teradata user password
	 */
	private String password;
	/**
	 * Location for final files
	 */
	private String outputDirectory;
	/**
	 * Cache directory for slices
	 */
	private String cacheDirectory;
	/**
	 * Directory for logging out put. Defaults to 'logs'
	 */
	private String logDirectory = "logs";
	/**
	 * Json file representing the data base schema
	 */
	private String schemaFile;
	/**
	 * Number of threads to be used
	 */
	private int threadCount;
	/**
	 * View export definition
	 */
	private ExportConfig export;
	/**
	 * Configuration for decode file creation
	 */
	private List<DecodingConfig> decode;
	/**
	 * Configuration for appending new columns
	 */
	private List<AppendConfiguration> transformers;
	
	/**
	 * Filter settings for cleaning phase
	 */
	private List<FilterConfig> filters;
	
	private CCIConfig cci;
	
	private StatisticConfig statistics;
	
	/**
	 * Automatically filled.
	 */
	@JsonIgnore
	private String fullConnectionUrl;
	
	public static Configuration loadFromJson(String path) throws JsonProcessingException, IOException {
		Configuration config =  new ObjectMapper().readValue(new File(path), Configuration.class);
				
		config.fullConnectionUrl = config.getUrl() + config.getHost() + "/" + config.getParameters();
		if(config.logDirectory == null || config.logDirectory.isEmpty())
			config.logDirectory = "logs";
		return config;
	}
}
