package de.ingef.eva.configuration;

import java.io.File;
import java.io.IOException;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import de.ingef.eva.database.DatabaseHost;
import de.ingef.eva.database.TextDatabase;
import de.ingef.eva.database.TextSchema;
import de.ingef.eva.database.TextTable;
import lombok.extern.log4j.Log4j2;

@Log4j2
public class ConfigurationDatabaseHostLoader implements DatabaseHostLoader {

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
				log.error("Cannot create schema object. No database entry found.");

		} catch (JsonProcessingException e) {
			log.error("Could not process Json-file '{}'.\n\tReason: {}.\n\tStackTrace: ", file, e.getMessage(), e);
		} catch (IOException e) {
			log.error("Could not read file '{}'.\n\tReason: {}.\n\tStackTrace: ", file, e.getMessage(), e);
		}
		return null;
	}
}
