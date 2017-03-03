package de.ingef.eva.database;

import java.util.ArrayList;
import java.util.Collection;

public class TextSchema implements DatabaseHost 
{
	private Collection<Database> _databases = new ArrayList<Database>();	

	@Override
	public Database findDatabaseByName(String name) {
		for(Database db : _databases)
		{
			if(db.getName().equalsIgnoreCase(name))
				return db;
		}
		return null;
	}

	@Override
	public Collection<Database> getAllDatabases() {
		return _databases;
	}

	public void addDatabase(Database db) {
		_databases.add(db);
	}
}
