package de.ingef.eva.configuration;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.util.Collection;

import org.junit.Test;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import de.ingef.eva.database.Database;
import de.ingef.eva.database.DatabaseHost;
import de.ingef.eva.database.Table;

public class ConfigurationDatabaseHostLoaderTest {

	@Test
	public void testLoadFromFile() throws JsonParseException, JsonMappingException, IOException {
		Configuration config = new ObjectMapper().readValue(new File("src/test/resources/configuration/dbHost.json"), Configuration.class);
		DatabaseHost dbh = new ConfigurationDatabaseHostLoader().createDatabaseHost(config);
		assertEquals(2, dbh.getAllDatabases().size());
		int i=1;
		for(Database db : dbh.getAllDatabases())
		{
			assertEquals("database"+i,db.getName());
			Collection<Table> tables = db.getAllTables();
			assertEquals(1*i, tables.size());
			int ti=i;
			for(Table t : tables)
			{
				assertEquals("table"+ti++, t.getName());
			}
			i++;
		}
	}

}
