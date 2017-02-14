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
		
		node = root.path("connectionUrl");
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
				ArrayList<String> viewNames = new ArrayList<String>(viewsNode.size());
				for(String viewName : viewNames)
				{
					viewNames.add(viewName);
				}
				
				databaseViews.put(name, viewNames);
			}
			
			databaseQueryConfiguration = new DatabaseQueryConfiguration(startYear, endYear, databaseViews);
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
}
