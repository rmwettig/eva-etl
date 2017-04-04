package de.ingef.eva.configuration;

import java.util.ArrayList;
import java.util.Collection;
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
	private FastExportConfiguration _fastExportConfiguration;
	private JsonNode _databasesNode;
	private Collection<Mapping> _mappings; 
	
	public Configuration(JsonNode root) {
		prepareConnectionConfiguration(root);
		prepareMappingConfiguration(root);
		_fastExportConfiguration = new FastExportConfiguration(root);
		
		JsonNode databaseNode = root.path("databases");
		if (databaseNode.isMissingNode())
			System.out.println("Missing 'databases' configuration entry");
		_databasesNode = databaseNode;
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
		_threadCount = node.asInt(1);
		
		if(_threadCount < 0) _threadCount = 1;
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
