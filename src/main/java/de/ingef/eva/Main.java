package de.ingef.eva;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import de.ingef.eva.configuration.Configuration;
import de.ingef.eva.configuration.ConfigurationReader;
import de.ingef.eva.configuration.DatabaseEntry;
import de.ingef.eva.configuration.JsonConfigurationReader;
import de.ingef.eva.processor.RemoveNewlineCharacters;
import de.ingef.eva.processor.Processor;
import de.ingef.eva.processor.RemoveBoundaryWhitespaces;
import de.ingef.eva.utility.Helper;

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
				
				Processor<StringBuilder> newlineRemover = new RemoveNewlineCharacters();
				Processor<StringBuilder> whitespaceTrimmer = new RemoveBoundaryWhitespaces(); 
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
							
							String part = String.format(".part%d.", pid++);
							String outfileName = String.format("%s/out/query_%s_%s%s.csv", workingDirectory, dbe.getName(), table, part);
							BufferedWriter writer = new BufferedWriter(new FileWriter(outfileName));
							System.out.println(String.format("Creating result file %s...", outfileName));
							while(result.next())
							{
								//process column entries immediately
								StringBuilder row = new StringBuilder();
								int columnCount = result.getMetaData().getColumnCount();
								for(int i = 1; i <= columnCount; i++)
								{
									StringBuilder entry = new StringBuilder(result.getString(i));
									entry = newlineRemover.process(entry);
									entry = whitespaceTrimmer.process(entry);
									row.append(entry);
									if(i != columnCount)
									{
										row.append(";");
									}
								}								
								writer.write(row.toString());
								writer.newLine();
							}
							writer.close();
							System.out.println(String.format("\tDone in %d min.", Helper.NanoSecondsToMinutes(System.nanoTime() - queryTimeTaken)));
							result.close();
						}
						preparedStatement.close();
					}
				}
			
				System.out.println(String.format("Time taken: %d min.", Helper.NanoSecondsToMinutes(System.nanoTime() - start)));
				connection.close();
				System.out.println("Done.");
			}
			catch(ClassNotFoundException cnfe){
				System.out.println("Error: Could not load database driver: " + cnfe.getMessage());
			} catch (SQLException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}

		}
	}	
}
