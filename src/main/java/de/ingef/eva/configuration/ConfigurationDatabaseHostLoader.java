package de.ingef.eva.configuration;

import java.io.File;
import java.io.IOException;

import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import de.ingef.eva.database.DatabaseHost;
import de.ingef.eva.database.TextDatabase;
import de.ingef.eva.database.TextSchema;
import de.ingef.eva.database.TextTable;

public class ConfigurationDatabaseHostLoader implements DatabaseHostLoader {

	private Logger _logger;

	public ConfigurationDatabaseHostLoader() {
	}

	public ConfigurationDatabaseHostLoader(Logger logger) {
		_logger = logger;
	}

	/**
	 * Creates a {@see DatabaseHost} object from a Configuration file structure.
	 * File must have the following structure at its root object: { ...,
	 * "databases": { ..., "sources": [ { "name":"value", "views": [
	 * {"tablename":{...}} ] }, { "name":"value", "views": [ {"tablename":{...}}
	 * ] } ] }
	 */
	@Override
	public DatabaseHost loadFromFile(String file) {

		try {
			ObjectMapper mapper = new ObjectMapper();
			JsonNode root = mapper.readTree(new File(file));
			JsonNode dbNode = root.path("databases").path("sources");

			if (!dbNode.isMissingNode() && dbNode.isArray()) {
				TextSchema schema = new TextSchema();
				for (JsonNode source : dbNode) {
					JsonNode node = source.path("name");
					if (!node.isMissingNode()) {
						String dbName = node.asText();
						TextDatabase db = new TextDatabase(dbName);
						node = source.path("views");
						if (!node.isMissingNode() && node.isArray()) {
							for (JsonNode view : node) {
								db.addTable(new TextTable(view.fieldNames().next()));
							}
						}
						schema.addDatabase(db);
					}
				}
				return schema;
			} else
				log("Cannot create schema object. No database entry found.");

		} catch (JsonProcessingException e) {
			log(e);
		} catch (IOException e) {
			log(e);
		}
		return null;
	}

	private void log(Exception e) {
		if (_logger != null)
			_logger.error("Error occured: {}.\n{}", e.getMessage(), e.getStackTrace());
		else
			e.printStackTrace();
	}

	private void log(String message) {
		if (_logger != null)
			_logger.error(message);
	}

}
