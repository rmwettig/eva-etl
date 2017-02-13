package de.ingef.eva.configuration;

public class Configuration {
	
	private String server;
	private String connectionUrl;
	private String username;
	private String userpassword;

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

	public String getUrl() {
		return connectionUrl;
	}
	
	public void setUrl(String url) {
		this.connectionUrl = url;
	}

	public String getServer() {
		return server;
	}

	public void setServer(String server) {
		this.server = server;
	}
	
	public String createFullConnectionUrl() {
		return connectionUrl + server;
	}
}
