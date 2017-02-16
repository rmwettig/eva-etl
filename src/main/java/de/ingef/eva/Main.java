package de.ingef.eva;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;

import de.ingef.eva.configuration.Configuration;
import de.ingef.eva.configuration.ConfigurationReader;
import de.ingef.eva.configuration.DatabaseEntry;
import de.ingef.eva.configuration.JsonConfigurationReader;
import de.ingef.eva.processor.RemoveNewlineCharacters;
import de.ingef.eva.processor.Processor;
import de.ingef.eva.utility.Helper;
import de.ingef.eva.writer.CsvWriter;
import de.ingef.eva.writer.ResultWriter;

public class Main {

	public static void main(String[] args) {
		final String workingDirectory = System.getProperty("user.dir");
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
				
				int[] years = Helper.extractYears(configuration.getDatabaseQueryConfiguration());
				ResultWriter writer = new CsvWriter();
				Processor resultProcessor = new RemoveNewlineCharacters();
				int debugBreak = 0;
				long start = System.nanoTime();
	
				for(DatabaseEntry dbe : configuration.getDatabaseQueryConfiguration().getEntries())
				{
					for(String table : dbe.getTables())
					{
						String query = String.format(dbe.getFetchQuery(), dbe.getName(), table);
						PreparedStatement preparedStatement = connection.prepareStatement(query);
						if(debugBreak++ == 2) break; //only run 3 queries for testing
						long queryTimeTaken = System.nanoTime();
						int pid = 0;
						for(int year : years)
						{
							preparedStatement.setInt(1, year);
							System.out.println("Executing query...\n"+query);
							ResultSet result = preparedStatement.executeQuery();
							
							while(result.next())
							{
								//TODO process column entries immediately
								System.out.println("Processing results...");
//								rows = resultProcessor.process(rows);
								System.out.println("Writing results...");
								String part = String.format(".part%d.", pid++);
//								writer.Write(rows, String.format("%s/out/query_%s_%s%s.csv", workingDirectory, dbe.getName(), table, part));
							}
							System.out.println(String.format("Time taken: %d min.", Helper.NanoSecondsToMinutes(System.nanoTime() - queryTimeTaken)));
							
							result.close();
						}
						preparedStatement.close();
					}
				}
			
				//String query = "SELECT * from ACC_FDB.AVK_FDB_T_KH_OPS sample 10;";
				System.out.println("Time taken (ns)" + (System.nanoTime() - start));
				connection.close();
				System.out.println("Done.");
			}
			catch(ClassNotFoundException cnfe){
				System.out.println("Error: Could not load database driver: " + cnfe.getMessage());
			} catch (SQLException e) {
				e.printStackTrace();
			}

		}
	}	
}
