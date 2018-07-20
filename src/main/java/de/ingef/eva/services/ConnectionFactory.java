package de.ingef.eva.services;

import java.sql.Connection;
import java.sql.SQLException;

import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;

@Log4j2
@RequiredArgsConstructor
public abstract class ConnectionFactory {

	protected final String username;
	protected final String password;
	protected final String connectionUrl;
	private final String jdbcDriver;
	
	/**
	 * creates a new db connection. Factory must be initialized by calling {@link #initialize()}
	 * @return
	 */
	public synchronized Connection createConnection() throws SQLException {
		return instantiateConnection();
	}
	
	protected abstract Connection instantiateConnection() throws SQLException;
	
	public boolean initialize() {
		return loadDriver();
	}
		
	private boolean loadDriver() {
		try {
			Class.forName(jdbcDriver);
			return true;
		} catch (ClassNotFoundException e) {
			log.error("Could not load JDBC driver '{}'", e);
			return false;
		}
	}
}
