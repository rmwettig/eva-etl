package de.ingef.eva.configuration;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import de.ingef.eva.database.Column;
import de.ingef.eva.database.Database;
import de.ingef.eva.database.DatabaseHost;
import de.ingef.eva.database.Table;
import de.ingef.eva.database.TextColumn;
import de.ingef.eva.database.TextDatabase;
import de.ingef.eva.database.TextSchema;
import de.ingef.eva.database.TextTable;

public class SchemaDatabaseHostLoader implements DatabaseHostLoader {

	/**
	 * Processes a JSON file with the following structure: { "databasename": {
	 * "tablename": ["columnname1", "columnname2" ...], "tablename2": [...], },
	 * "databasename2":{...} }
	 * 
	 * @param root
	 */
	@Override
	public DatabaseHost loadFromFile(String file) {
		ObjectMapper mapper = new ObjectMapper();
		try {
			JsonNode root = mapper.readTree(new File(file));
			TextSchema schema = new TextSchema();
			Iterator<String> dbIter = root.fieldNames();

			while (dbIter.hasNext()) {
				String dbName = dbIter.next();
				Database db = new TextDatabase(dbName);
				JsonNode tables = root.path(dbName);
				Iterator<String> tableIter = tables.fieldNames();
				while (tableIter.hasNext()) {
					String tableName = tableIter.next();
					Table t = new TextTable(tableName);
					JsonNode columns = tables.path(tableName);
					for (JsonNode column : columns) {
						Column c = new TextColumn(column.path("column").asText(), column.path("type").asText());
						t.addColumn(c);
					}
					db.addTable(t);
				}
				schema.addDatabase(db);
			}

			return schema;
		} catch (JsonProcessingException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}

}
