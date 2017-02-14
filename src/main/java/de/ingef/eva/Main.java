package de.ingef.eva;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;

import de.ingef.eva.configuration.Configuration;
import de.ingef.eva.configuration.ConfigurationReader;
import de.ingef.eva.configuration.JsonConfigurationReader;
import de.ingef.eva.processor.CleanRowsResultProcessor;
import de.ingef.eva.processor.ResultProcessor;
import de.ingef.eva.writer.CsvWriter;
import de.ingef.eva.writer.ResultWriter;

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
				String query = "SELECT * from ACC_FDB.AVK_FDB_T_KH_OPS sample 10;";
				System.out.println("Executing query...");
				ResultSet result = sqlStatement.executeQuery(query);
				
				System.out.println("Processing results...");
				ResultProcessor resultProcessor = new CleanRowsResultProcessor();
				Collection<String[]> cleanRows = resultProcessor.ProcessResults(convertResultSet(result));
				
				System.out.println("Writing results...");
				ResultWriter writer = new CsvWriter();
				writer.Write(cleanRows, "dummyout.csv");
				sqlStatement.close();
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
	
	private static Collection<String[]> convertResultSet(ResultSet results)
	{
		ArrayList<String[]> converted = new ArrayList<String[]>(1000);
		try {
			String[] names = extractColumnNames(results);
			if(names != null)
				converted.add(names);
			int columnCount = results.getMetaData().getColumnCount();
			while(results.next())
			{
				String[] row = new String[columnCount];
				for(int i = 0; i < columnCount; i++)
				{
					//index of sql set starts at 1
					String content = results.getString(i+1); 
					row[i] = content != null ? content : "";
				}
				converted.add(row);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return converted;
	}
	
	private static String[] extractColumnNames(ResultSet results)
	{
		String[] names = null;
		try {
			ResultSetMetaData metadata = results.getMetaData();
			int columnCount = metadata.getColumnCount();
			names = new String[columnCount];
			for(int i = 0; i < columnCount; i++)
				names[i] = metadata.getColumnName(i+1);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return names;
	}

}
