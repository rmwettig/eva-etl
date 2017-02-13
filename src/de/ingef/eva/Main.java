package de.ingef.eva;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.util.Enumeration;

 

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.sql.DataSource;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import de.ingef.eva.configuration.Configuration;
import de.ingef.eva.configuration.ConfigurationReader;
import de.ingef.eva.configuration.JsonConfigurationReader;

public class Main {

	public static void main(String[] args) {
		//TODO: read from config json
		final String user ="";
		final String password = "";
		final String configFilePath = args[0];
		if(!configFilePath.isEmpty()){
			
			ConfigurationReader configReader = new JsonConfigurationReader();
			Configuration configuration = configReader.ReadConfiguration(configFilePath);
			
			
			Enumeration<Driver> dvs = DriverManager.getDrivers();
			while(dvs.hasMoreElements())
				System.out.println(dvs.nextElement().getClass().getName());
			
			try{
				//Class.forName("com.teradata.jdbc.TeraDriver");
				
				Context ctx = new InitialContext();
				DataSource ds = (DataSource) ctx.lookup( "odbc/teradata" );
				//Connection con = ds.getConnection( "username", "password" );
			
			}
	//		catch(ClassNotFoundException cnfe)
	//		{
	//			System.out.println("Error: " + cnfe.getMessage());
	//		}
			catch(NamingException ne)
			{
				//System.out.println("Error: "+ ne.getMessage());
			}
		}
	}

}
