package de.ingef.eva;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import de.ingef.eva.configuration.Configuration;
import de.ingef.eva.configuration.ConfigurationReader;
import de.ingef.eva.configuration.JsonConfigurationReader;

public class Main {

	public static void main(String[] args) {

		final String configFilePath = args[0];
		if(!configFilePath.isEmpty()){

			ConfigurationReader configReader = new JsonConfigurationReader();
			Configuration configuration = configReader.ReadConfiguration(configFilePath);


			try {
				Class.forName("com.teradata.jdbc.TeraDriver");
				Connection connection = DriverManager.getConnection(
						configuration.createFullConnectionUrl(),
						configuration.getUsername(),
						configuration.getUserpassword()
					);
				Statement sqlStatement = connection.createStatement();
				String query = "SELECT * from table LIMIT 10;";
				ResultSet result = sqlStatement.executeQuery(query);
				sqlStatement.close();
				connection.close();
			}
			catch(ClassNotFoundException cnfe){
				System.out.println("Error: Could not load database driver: " + cnfe.getMessage());
			} catch (SQLException e) {
				e.printStackTrace();
			}

		}
	}

}
