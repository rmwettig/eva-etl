package de.ingef.eva.configuration;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.HashMap;

import com.fasterxml.jackson.databind.JsonNode;

public class Configuration {
	
	private String server;
	private String connectionUrl;
	private String username;
	private String userpassword;
	private String outDirectory;
	private String tempDirectory;
	private DatabaseQueryConfiguration databaseQueryConfiguration;
	
	public Configuration(JsonNode root)
	{
		PrepareConnectionConfiguration(root);
		PrepareDatabaseConfiguration(root);		
	}
	
	public String createFullConnectionUrl() {
		return connectionUrl + server;
	}
	
	private void PrepareConnectionConfiguration(JsonNode root)
	{
		JsonNode node = root.path("server");
		if(node.isMissingNode())
			System.out.println("Missing 'server' configuration entry.");
		server = node.asText();
		
		node = root.path("url");
		if(node.isMissingNode())
			System.out.println("Missing 'connectionUrl' configuration entry.");
		connectionUrl = node.asText();
		
		node = root.path("username");
		if(node.isMissingNode())
			System.out.println("Missing 'username' configuration entry.");
		username = node.asText();
		
		node = root.path("userpassword");
		if(node.isMissingNode())
			System.out.println("Missing 'userpassword' configuration entry.");
		userpassword = node.asText();
		
		node = root.path("outputdirectory");
		if(node.isMissingNode())
			System.out.println("Missing 'outdirectory' configuration entry.");
		outDirectory = node.asText();
		
		node = root.path("tempdirectory");
		if(node.isMissingNode())
			System.out.println("Missing 'tempdirectory' configuration entry.");
		tempDirectory = node.asText();
	}
	
	private void PrepareDatabaseConfiguration(JsonNode root) 
	{
		JsonNode databaseNode = root.path("databases");
		if(databaseNode.isMissingNode())
			System.out.println("Missing 'databases' configuration entry");
		JsonNode node = databaseNode.path("startYear");
		if(node.isMissingNode())
			System.out.println("Missing 'startYear' configuration entry");
		int startYear = node.asInt();
		
		int defaultEndYear = Calendar.getInstance().get(Calendar.YEAR);
		node = databaseNode.path("endYear");
		if(node.isMissingNode())
			System.out.println("Missing 'endYear' configuration entry. Using default value: "+  defaultEndYear);
		int endYear = node.asInt(defaultEndYear);
		Collection<DatabaseEntry> entries = new ArrayList<DatabaseEntry>();
		node = databaseNode.path("sources");
		if(!node.isMissingNode() && node.isArray())
		{
			HashMap<String, Collection<String>> databaseViews = new HashMap<String, Collection<String>>();
			//for all databases
			for(JsonNode source : node)
			{
				//get the name and associated views
				String name = source.path("name").asText();
				JsonNode viewsNode = source.path("views");
				//skip current processing if views is not an array
				if(!viewsNode.isArray())
				{
					System.out.println(String.format("Views entry is not an array. Skipping '%s'.", name));
					continue;
				}
				
				//extract view names and save in hashmap
				Collection<String> viewNames = new ArrayList<String>(viewsNode.size());
				for(JsonNode viewNode : viewsNode)
				{
					viewNames.add(viewNode.asText());
				}
				
				String fetchQuery = source.path("fetchby").asText();
				
				entries.add(new DatabaseEntry(name, fetchQuery, viewNames));
			}
			
			databaseQueryConfiguration = new DatabaseQueryConfiguration(startYear, endYear, entries);
		}
		else
		{
			System.out.println("Missing 'sources' configuration entry or is not an array.");
		}
	}

	public String getServer() {
		return server;
	}

	public void setServer(String server) {
		this.server = server;
	}

	public String getConnectionUrl() {
		return connectionUrl;
	}

	public void setConnectionUrl(String connectionUrl) {
		this.connectionUrl = connectionUrl;
	}

	public String getUsername() {
		return username;
	}

	public void setUsername(String username) {
		this.username = username;
	}

	public String getUserpassword() {
		return userpassword;
	}

	public void setUserpassword(String userpassword) {
		this.userpassword = userpassword;
	}

	public DatabaseQueryConfiguration getDatabaseQueryConfiguration() {
		return databaseQueryConfiguration;
	}

	public void setDatabaseQueryConfiguration(DatabaseQueryConfiguration databaseQueryConfiguration) {
		this.databaseQueryConfiguration = databaseQueryConfiguration;
	}

	public String getOutDirectory() {
		return outDirectory;
	}

	public String getTempDirectory() {
		return tempDirectory;
	}
}
