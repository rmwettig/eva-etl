package de.ingef.eva.configuration;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import com.fasterxml.jackson.databind.JsonNode;


public class Configuration {

	private String _server;
	private String _connectionUrl;
	private String _connectionParameters;
	private String _username;
	private String _password;
	private String _outDirectory;
	private String _tempDirectory;
	private String _schemaFilePath;
	private int _threadCount;
	private DatabaseQueryConfiguration _databaseQueryConfiguration;
	private FastExportConfiguration _fastExportConfiguration;
	private JsonNode _databasesNode;
	private Collection<Mapping> _mappings; 
	
	public Configuration(JsonNode root) {
		prepareConnectionConfiguration(root);
		prepareDatabaseConfiguration(root);
		prepareMappingConfiguration(root);
		_fastExportConfiguration = new FastExportConfiguration(root);
	}

	public String createFullConnectionUrl() {
		return String.format("%s%s/%s", _connectionUrl, _server, _connectionParameters);
	}

	private void prepareConnectionConfiguration(JsonNode root) {
		JsonNode node = root.path("server");
		if (node.isMissingNode())
			System.out.println("Missing 'server' configuration entry.");
		_server = node.asText();

		node = root.path("url");
		if (node.isMissingNode())
			System.out.println("Missing 'connectionUrl' configuration entry.");
		_connectionUrl = node.asText();

		node = root.path("parameters");
		_connectionParameters = (!node.isMissingNode()) ? node.asText() : "";

		node = root.path("username");
		if (node.isMissingNode())
			System.out.println("Missing 'username' configuration entry.");
		_username = node.asText();

		node = root.path("userpassword");
		if (node.isMissingNode())
			System.out.println("Missing 'userpassword' configuration entry.");
		_password = node.asText();

		node = root.path("outputdirectory");
		if (node.isMissingNode())
			System.out.println("Missing 'outdirectory' configuration entry.");
		_outDirectory = node.asText();

		node = root.path("tempdirectory");
		if (node.isMissingNode())
			System.out.println("Missing 'tempdirectory' configuration entry.");
		_tempDirectory = node.asText();

		node = root.path("schemafile");
		if (node.isMissingNode())
			System.out.println("Missing 'schemafile' configuration entry.");
		_schemaFilePath = node.asText();

		node = root.path("threads");
		if (node.isMissingNode())
			System.out.println("Missing 'threads' configuration entry.");

		try {
			_threadCount = Integer.parseInt(node.asText());
			if (_threadCount < 1)
				_threadCount = 1;
		} catch (NumberFormatException e) {
			System.out.println("Error: 'threadCount' has an invalid value.");
			_threadCount = 1;
		}

	}

	private void prepareDatabaseConfiguration(JsonNode root) {
		JsonNode databaseNode = root.path("databases");
		if (databaseNode.isMissingNode())
			System.out.println("Missing 'databases' configuration entry");
		_databasesNode = databaseNode;
		JsonNode node = databaseNode.path("startYear");
		if (node.isMissingNode())
			System.out.println("Missing 'startYear' configuration entry");
		int startYear = node.asInt();

		int defaultEndYear = Calendar.getInstance().get(Calendar.YEAR);
		node = databaseNode.path("endYear");
		if (node.isMissingNode())
			System.out.println("Missing 'endYear' configuration entry. Using default value: " + defaultEndYear);
		int endYear = node.asInt(defaultEndYear);
		Collection<DatabaseEntry> entries = new ArrayList<DatabaseEntry>();
		node = databaseNode.path("sources");
		if (!node.isMissingNode() && node.isArray()) {
			// for all databases
			for (JsonNode source : node) {
				// get the name and associated views
				String name = source.path("name").asText();
				JsonNode viewsNode = source.path("views");
				// skip current processing if views is not an array
				if (!viewsNode.isArray()) {
					System.out.println(String.format("Views entry is not an array. Skipping '%s'.", name));
					continue;
				}

				// extract view names and save in hashmap
				Collection<String> viewNames = new ArrayList<String>(viewsNode.size());
				for (JsonNode viewNode : viewsNode) {
					viewNames.add(viewNode.asText());
				}

				Map<String, Collection<String>> globalConditions = null;
				JsonNode conditions = source.path("conditions");
				if (!conditions.isMissingNode()) {
					globalConditions = new HashMap<String, Collection<String>>();
					Iterator<Entry<String, JsonNode>> iter = conditions.fields();
					while (iter.hasNext()) {
						// this corresponds to a column name entry
						// associated with an array of values
						Entry<String, JsonNode> entry = iter.next();
						JsonNode entryContent = entry.getValue();
						Collection<String> values = new ArrayList<String>(entryContent.size());
						for (JsonNode j : entryContent)
							values.add(j.asText());
						globalConditions.put(entry.getKey(), values);
					}
				}
				entries.add(new DatabaseEntry(name, viewNames, globalConditions));
			}

			_databaseQueryConfiguration = new DatabaseQueryConfiguration(startYear, endYear, entries);
		} else {
			System.out.println("Missing 'sources' configuration entry or is not an array.");
		}
	}
	
	private void prepareMappingConfiguration(JsonNode root) {
		JsonNode mappings = root.path("mappings");
		
		if(mappings.isMissingNode()) return;
		
		_mappings = new ArrayList<Mapping>();
		
		for(JsonNode mapping : mappings) {
			JsonNode sourceNode = mapping.path("source");
			JsonNode sourceKeyColumnNode = mapping.path("sourceKeyColumn");
			JsonNode targetKeyColumnNode = mapping.path("targetKeyColumn");
			JsonNode targets = mapping.path("targets");
			
			if(	sourceNode.isMissingNode() ||
				sourceKeyColumnNode.isMissingNode() ||
				targetKeyColumnNode.isMissingNode() ||
				targets.isMissingNode()) return;
			
			Collection<Target> unmappedFiles = new ArrayList<Target>();
			
			for(JsonNode target : targets) {
				JsonNode data = target.path("data");
				JsonNode header = target.path("header");
				
				if(data.isMissingNode() || header.isMissingNode()) continue;
				
				unmappedFiles.add(new Target(header.asText(), data.asText()));
			}
			String source = sourceNode.asText();
			String sourceKeyColumn = sourceKeyColumnNode.asText();
			String targetKeyColumn = targetKeyColumnNode.asText();
			
			_mappings.add(new Mapping(source, sourceKeyColumn, targetKeyColumn, unmappedFiles));
		}
	}
	
	public String getServer() {
		return _server;
	}

	public void setServer(String server) {
		this._server = server;
	}

	public String getConnectionUrl() {
		return _connectionUrl;
	}

	public void setConnectionUrl(String connectionUrl) {
		this._connectionUrl = connectionUrl;
	}

	public String getUsername() {
		return _username;
	}

	public void setUsername(String username) {
		this._username = username;
	}

	public String getPassword() {
		return _password;
	}

	public void setPassword(String password) {
		this._password = password;
	}

	public DatabaseQueryConfiguration getDatabaseQueryConfiguration() {
		return _databaseQueryConfiguration;
	}

	public void setDatabaseQueryConfiguration(DatabaseQueryConfiguration databaseQueryConfiguration) {
		this._databaseQueryConfiguration = databaseQueryConfiguration;
	}

	public String getOutDirectory() {
		return _outDirectory;
	}

	public String getTempDirectory() {
		return _tempDirectory;
	}

	public int getThreadCount() {
		return _threadCount;
	}

	public FastExportConfiguration getFastExportConfiguration() {
		return _fastExportConfiguration;
	}

	public String getSchemaFilePath() {
		return _schemaFilePath;
	}

	public JsonNode getDatabaseNode() {
		return _databasesNode;
	}
	
	public Collection<Mapping> getMappings() {
		return _mappings;
	}
}
