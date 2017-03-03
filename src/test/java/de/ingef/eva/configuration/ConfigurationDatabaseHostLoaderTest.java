package de.ingef.eva.configuration;

import static org.junit.Assert.*;

import java.util.Collection;

import org.junit.Test;

import de.ingef.eva.database.Database;
import de.ingef.eva.database.DatabaseHost;
import de.ingef.eva.database.Table;

public class ConfigurationDatabaseHostLoaderTest {

	@Test
	public void testLoadFromFile() {
		DatabaseHost dbh = new ConfigurationDatabaseHostLoader().loadFromFile("src/test/resources/configuration/dbHost.json");
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
