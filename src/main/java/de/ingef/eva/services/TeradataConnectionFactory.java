package de.ingef.eva.services;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class TeradataConnectionFactory extends ConnectionFactory {

	public TeradataConnectionFactory(String username, String password, String connectionUrl) {
		super(username, password, connectionUrl, "com.teradata.jdbc.TeraDriver");
	}

	@Override
	protected Connection instantiateConnection() throws SQLException {
		return DriverManager.getConnection(
				connectionUrl,
				username,
				password
		);
	}

	

}
