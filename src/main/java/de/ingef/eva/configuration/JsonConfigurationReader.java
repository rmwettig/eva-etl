package de.ingef.eva.configuration;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import de.ingef.eva.error.InvalidConfigurationException;

/**
 * Reads a json as a tree. This allows to use nested objects like arrays and
 * json-objects
 * 
 * @author Martin Wettig
 *
 */
public class JsonConfigurationReader implements ConfigurationReader {

	public Configuration ReadConfiguration(String location) {
		Configuration config = null;
		try {
			config = Configuration.loadFromJson(location);
		} catch (JsonParseException jpe) {
			System.out.println("Error while parsing configuration JSON file.\nError: " + jpe.getMessage());
		} catch (JsonMappingException jme) {
			System.out.println("Error while mapping configuration JSON file.\nError: " + jme.getMessage());
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InvalidConfigurationException e) {
			e.printStackTrace();
		}
		return config;
	}

}
