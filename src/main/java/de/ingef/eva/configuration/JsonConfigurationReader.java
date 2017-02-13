package de.ingef.eva.configuration;

import java.io.File;
import java.io.IOException;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class JsonConfigurationReader implements ConfigurationReader {

	public Configuration ReadConfiguration(String location) {
		Configuration config = null;
		try{
			ObjectMapper mapper = new ObjectMapper();
			config = mapper.readValue(new File(location), Configuration.class);
		}catch(JsonParseException jpe)
		{
			System.out.println("Error while parsing configuration JSON file.\nError: " + jpe.getMessage());
		} catch (JsonMappingException jme) {
			System.out.println("Error while mapping configuration JSON file.\nError: " + jme.getMessage());
		} catch (IOException e) {
			e.printStackTrace();
		}
		return config;
	}

}
