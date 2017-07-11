package de.ingef.eva.configuration;

import static org.junit.Assert.*;

import java.util.Collection;

import org.junit.Test;

import de.ingef.eva.data.TeradataColumnType;
import de.ingef.eva.database.Column;
import de.ingef.eva.database.Database;
import de.ingef.eva.database.DatabaseHost;
import de.ingef.eva.database.Table;

public class SchemaDatabaseHostLoaderTest {

	@Test
	public void testLoadFromFile() {
		DatabaseHost dbh = new SchemaDatabaseHostLoader().loadFromFile("src/test/resources/configuration/schema.json");
		assertEquals(2, dbh.getAllDatabases().size());
		int i = 1;
		for(Database db : dbh.getAllDatabases())
		{
			if(i==1)
			{
				assertEquals("database1", db.getName());
				Collection<Table> tables = db.getAllTables();
				assertEquals(2, tables.size());
				int j = 1;
				for(Table t: tables)
				{
					if(j==1)
					{
						assertEquals("tablename", t.getName());
						Collection<Column> columns = t.getAllColumns();
						assertEquals(2, columns.size());
						int k = 1;
						for(Column c : columns)
						{
							if(k==1) {
								assertEquals("columnname1", c.getName());
								assertEquals(TeradataColumnType.CHARACTER, c.getType());
							}
							if(k==2) {
								assertEquals("columnname2", c.getName());
								assertEquals(TeradataColumnType.INTEGER, c.getType());
							}
							k++;
						}
					}
					if(j==2)
					{
						assertEquals("tablename2", t.getName());
						Collection<Column> columns = t.getAllColumns();
						assertEquals(1, columns.size());
						for(Column c : columns)
						{
							assertEquals("columnname3", c.getName());
							assertEquals(TeradataColumnType.CHARACTER, c.getType());
						}
					}
					j++;
				}
			}
			if(i==2)
			{
				assertEquals("database2", db.getName());
				Collection<Table> tables = db.getAllTables();
				assertEquals(1, tables.size());
				int j = 1;
				for(Table t : tables)
				{
					if(j==1)
					{
						assertEquals("tablename3", t.getName());
						Collection<Column> columns = t.getAllColumns();
						assertEquals(1, columns.size());
						for(Column c : columns)
						{
							assertEquals("columnname4", c.getName());
							assertEquals(TeradataColumnType.INTEGER, c.getType());
						}
					}
				}
			}
			i++;
		}
	}

}
