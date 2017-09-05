package de.ingef.eva.configuration;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import de.ingef.eva.configuration.append.AppendConfiguration;
import de.ingef.eva.data.TeradataColumnType;
import de.ingef.eva.data.validation.NameRule;
import de.ingef.eva.data.validation.ReplacePattern;
import de.ingef.eva.data.validation.Rule;
import de.ingef.eva.data.validation.TypeRule;
import de.ingef.eva.dataprocessor.SeparationMapping;
import de.ingef.eva.error.InvalidConfigurationException;
import lombok.Getter;

@Getter
public class Configuration {
	private String host;
	private String connectionUrl;
	private String connectionParameters;
	private String user;
	private String password;
	private String outputDirectory;
	private String tempDirectory;
	private String logDirectory = "logs";
	private String schemaFile;
	private int threadCount;
	private FastExportConfiguration fastExportConfiguration;
	private JsonNode databasesNode;
	private Collection<Mapping> mappings; 
	private Map<String,List<String>> nameToH2ik;
	private Collection<Rule> rules;
	private String fullConnectionUrl;
	private SeparationMapping datasetMembership;
	private List<AppendConfiguration> appenderConfiguration;
	
	public static Configuration loadFromJson(String path) throws JsonProcessingException, IOException, InvalidConfigurationException {
		String SERVER = "server";
		String URL = "url";
		String PARAMETERS = "parameters";
		String USER ="username";
		String PASSWORD = "userpassword";
		String OUTPUT_DIRECTORY = "outputdirectory";
		String TEMP_DIRECTORY = "tmpdirectory";
		String SCHEMAFILE = "schemafile";
		String THREADS = "threads";
		String DATABASES = "databases";
		
		ObjectMapper mapper = new ObjectMapper();
		JsonNode root = mapper.readTree(new File(path));
		Configuration config = new Configuration();
		
		JsonNode node = root.path(SERVER);
		if (node.isMissingNode()) throw new InvalidConfigurationException(String.format(ErrorMessage.MISSING_FIELD, SERVER));
		config.host = node.asText();
		
		node = root.path(URL);
		if (node.isMissingNode()) throw new InvalidConfigurationException(String.format(ErrorMessage.MISSING_FIELD, URL));
		config.connectionUrl = node.asText();

		node = root.path(PARAMETERS);
		config.connectionParameters = (!node.isMissingNode()) ? node.asText() : "";
		
		config.fullConnectionUrl = config.connectionUrl + config.host + "/" + config.connectionParameters;
		
		node = root.path(USER);
		if (node.isMissingNode()) throw new InvalidConfigurationException(String.format(ErrorMessage.MISSING_FIELD, USER));
		config.user = node.asText();

		node = root.path(PASSWORD);
		if (node.isMissingNode()) throw new InvalidConfigurationException(String.format(ErrorMessage.MISSING_FIELD, PASSWORD));
		config.password = node.asText();
		
		node = root.path("outputdirectory");
		if (node.isMissingNode()) throw new InvalidConfigurationException(String.format(ErrorMessage.MISSING_FIELD, OUTPUT_DIRECTORY));
		config.outputDirectory = node.asText();

		node = root.path("tempdirectory");
		if (node.isMissingNode()) throw new InvalidConfigurationException(String.format(ErrorMessage.MISSING_FIELD, TEMP_DIRECTORY));
		config.tempDirectory = node.asText();

		node = root.path(SCHEMAFILE);
		if (node.isMissingNode()) throw new InvalidConfigurationException(String.format(ErrorMessage.MISSING_FIELD, SCHEMAFILE));
		config.schemaFile = node.asText();

		node = root.path("threads");
		if (node.isMissingNode()) throw new InvalidConfigurationException(String.format(ErrorMessage.MISSING_FIELD, THREADS));
		int intendedThreadCount = node.asInt();
		if (intendedThreadCount < 1) throw new InvalidConfigurationException(String.format(ErrorMessage.INVALID_VALUE, THREADS, intendedThreadCount));
		config.threadCount = intendedThreadCount; 
				
		config.fastExportConfiguration = FastExportConfiguration.loadFromJson(root);
		
		JsonNode databaseNode = root.path("databases");
		if (databaseNode.isMissingNode()) throw new InvalidConfigurationException(String.format(ErrorMessage.MISSING_FIELD, DATABASES));
		config.databasesNode = databaseNode;
		
		config.mappings = prepareMappingConfiguration(root);
		config.nameToH2ik = prepareFilteringConfiguration(root);
		config.rules = prepareValidationRules(root);
		config.datasetMembership = prepareSeparationMappings(root);		
		config.appenderConfiguration = prepareAppendConfiguration(root, mapper);
		return config;
	}
	
	private static List<AppendConfiguration> prepareAppendConfiguration(JsonNode root, ObjectMapper mapper) throws InvalidConfigurationException {
		JsonNode appendNode = root.path("append");
		List<AppendConfiguration> configurations = new ArrayList<>(appendNode.size());
		for(JsonNode conf : appendNode) {
			try {
				configurations.add(mapper.treeToValue(conf, AppendConfiguration.class));
			} catch (JsonProcessingException e) {
				throw new InvalidConfigurationException("Could not read element in 'append'.", e);
			}
		}
		
		return configurations;
	}

	/**
	 * Parses separation mappings field
	 * @param root
	 * @return empty mapping if field is missing
	 */
	private static SeparationMapping prepareSeparationMappings(JsonNode root) {
		String SEPARATION = "separation";
		String DATASET = "dataset";
		String H2IKS = "h2iks";
		JsonNode mappings = root.path(SEPARATION);
		Map<String,String> mapping = new HashMap<>();
		if(mappings.isMissingNode())
			return new SeparationMapping(mapping);
		for(JsonNode map : mappings) {
			String datasetName = map.path(DATASET).asText("");
			if(datasetName.isEmpty()) continue;
			JsonNode h2iks = map.path(H2IKS);
			if(h2iks.isMissingNode()) continue;
			for(JsonNode h2ik : h2iks) {
				mapping.put(h2ik.asText(), datasetName);
			}
		}
		return new SeparationMapping(mapping);
	}

	private static Collection<Mapping> prepareMappingConfiguration(JsonNode root) throws InvalidConfigurationException {
		String MAPPINGS = "mappings";
		String SOURCE = "source";
		String SOURCE_KEY_COLUMN = "sourceKeyColumn";
		String TARGET_KEY_COLUMN = "targetKeyColumn";
		String TARGETS = "targets";
		String DATA = "data";
		String HEADER = "header";
		
		JsonNode mappingsNode = root.path(MAPPINGS);		
		if(mappingsNode.isMissingNode()) throw new InvalidConfigurationException(String.format(ErrorMessage.MISSING_FIELD, MAPPINGS));
		
		Collection<Mapping> mappings = new ArrayList<>();
		
		for(JsonNode mapping : mappingsNode) {
			JsonNode sourceNode = mapping.path(SOURCE);
			if(sourceNode.isMissingNode()) throw new InvalidConfigurationException(String.format(ErrorMessage.MISSING_FIELD, SOURCE));
			
			JsonNode sourceKeyColumnNode = mapping.path(SOURCE_KEY_COLUMN);
			if(sourceKeyColumnNode.isMissingNode()) throw new InvalidConfigurationException(String.format(ErrorMessage.MISSING_FIELD, SOURCE_KEY_COLUMN));
			
			JsonNode targetKeyColumnNode = mapping.path(TARGET_KEY_COLUMN);
			if(targetKeyColumnNode.isMissingNode()) throw new InvalidConfigurationException(String.format(ErrorMessage.MISSING_FIELD, TARGET_KEY_COLUMN));
			
			JsonNode targets = mapping.path(TARGETS);
			if(targets.isMissingNode()) throw new InvalidConfigurationException(String.format(ErrorMessage.MISSING_FIELD, TARGETS));
			
			Collection<Target> unmappedFiles = new ArrayList<>();
			
			for(JsonNode target : targets) {
				JsonNode data = target.path(DATA);
				if(data.isMissingNode()) throw new InvalidConfigurationException(String.format(ErrorMessage.MISSING_FIELD, DATA));
				
				JsonNode header = target.path(HEADER);
				if(header.isMissingNode()) throw new InvalidConfigurationException(String.format(ErrorMessage.MISSING_FIELD, HEADER));
								
				unmappedFiles.add(new Target(header.asText(), data.asText()));
			}
			String source = sourceNode.asText();
			String sourceKeyColumn = sourceKeyColumnNode.asText();
			String targetKeyColumn = targetKeyColumnNode.asText();
			
			mappings.add(new Mapping(source, sourceKeyColumn, targetKeyColumn, unmappedFiles));
		}
		
		return mappings;
	}
	
	private static Map<String,List<String>> prepareFilteringConfiguration(JsonNode root) throws InvalidConfigurationException {
		Map<String,List<String>> nameToH2ik = new HashMap<>();
		String DECODING = "decoding";
		String NAME = "name";
		String H2IK = "h2ik";
		
		JsonNode filteringNode = root.path(DECODING);
		if(filteringNode.isMissingNode()) throw new InvalidConfigurationException(String.format(ErrorMessage.MISSING_FIELD, DECODING));
		if(!filteringNode.isArray()) throw new InvalidConfigurationException(String.format(ErrorMessage.INVALID_TYPE, "Array"));
		
		for(JsonNode decoder : filteringNode) {
			JsonNode nameNode = decoder.path(NAME);
			if(nameNode.isMissingNode()) throw new InvalidConfigurationException(String.format(ErrorMessage.MISSING_FIELD, NAME));
			JsonNode h2ikNode = decoder.path(H2IK);
			if(h2ikNode.isMissingNode() || !h2ikNode.isArray()) throw new InvalidConfigurationException(String.format(ErrorMessage.MISSING_FIELD, H2IK));
			String name = nameNode.asText();
			if(name.isEmpty()) throw new InvalidConfigurationException(String.format(ErrorMessage.INVALID_VALUE, ""));
			List<String> h2iks = new ArrayList<>();
			for(JsonNode node : h2ikNode) {
				String h2ik = node.asText();
				if(h2ik.isEmpty()) throw new InvalidConfigurationException(String.format(ErrorMessage.INVALID_VALUE, ""));
				h2iks.add(h2ik);
			}
			
			nameToH2ik.put(name, h2iks);
		}
		
		return nameToH2ik;
	}
	
	private static Collection<Rule> prepareValidationRules(JsonNode root) throws InvalidConfigurationException {
		String VALIDATION = "validation";
		String RULE = "rule";
		String TYPE = "type";
		String EXCLUDE = "exclude";
		String REPLACE = "replace";
		String COLUMN = "column";
		
		JsonNode validationNode = root.path(VALIDATION);
		if(validationNode.isMissingNode()) throw new InvalidConfigurationException(String.format(ErrorMessage.MISSING_FIELD, VALIDATION));
		Collection<Rule> rules = new ArrayList<>();
		for(JsonNode rule : validationNode) {
			JsonNode ruleType = rule.path(RULE);
			if(ruleType.isMissingNode()) throw new InvalidConfigurationException(String.format(ErrorMessage.MISSING_FIELD, RULE));
			
			//exclude is a regex pattern that matches characters that must be ignored
			JsonNode excludeNode = rule.path(EXCLUDE);
			if(excludeNode.isMissingNode()) throw new InvalidConfigurationException(String.format(ErrorMessage.MISSING_FIELD, EXCLUDE));
			String excludePattern = excludeNode.asText();
			if(excludePattern.isEmpty()) throw new InvalidConfigurationException(String.format(ErrorMessage.INVALID_VALUE, ""));
			
			//optional replacement symbol
			String replaceText = rule.path(REPLACE).asText("");
			
			if(ruleType.asText().equalsIgnoreCase("TYPE"))
				rules.add(parseTypeRule(rule, TYPE, excludePattern, replaceText));
			
			if(ruleType.asText().equalsIgnoreCase("NAME"))
				rules.add(parseNameRule(rule, COLUMN, excludePattern, replaceText));
		}
		
		return rules;
	}
	
	private static Rule parseTypeRule(JsonNode rule, String typeKey, String excludePattern, String replaceText) throws InvalidConfigurationException {
		//type is the column data type
		JsonNode typeNode = rule.path(typeKey);
		if(typeNode.isMissingNode()) throw new InvalidConfigurationException(String.format(ErrorMessage.MISSING_FIELD, typeKey));
		String typeName = typeNode.asText();
		if(typeName.isEmpty()) throw new InvalidConfigurationException(String.format(ErrorMessage.INVALID_VALUE, ""));
		TeradataColumnType type = TeradataColumnType.fromTypeName(typeName);
		
		return new TypeRule(type, new ReplacePattern(excludePattern, replaceText));
	}
	
	private static Rule parseNameRule(JsonNode rule, String columnKey, String excludePattern, String replaceText) throws InvalidConfigurationException {
		JsonNode columnNameNode = rule.path(columnKey);
		if(columnNameNode.isMissingNode()) throw new InvalidConfigurationException(String.format(ErrorMessage.MISSING_FIELD, columnKey));
		String columnName = columnNameNode.asText();
		if(columnName == null || columnName.isEmpty()) throw new InvalidConfigurationException(String.format(ErrorMessage.INVALID_VALUE, columnName));
		
		return new NameRule(columnName, new ReplacePattern(excludePattern, replaceText));
	}
}
