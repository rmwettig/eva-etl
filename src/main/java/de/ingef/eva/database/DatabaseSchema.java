package de.ingef.eva.database;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

import lombok.ToString;

/**
 * Database lookups
 */
@ToString
public class DatabaseSchema {
	
	private List<Database> _databases = new ArrayList<>();
		
	public Optional<Database> findDatabaseByName(String name) {
		return _databases.stream().filter(db -> db.getName().equalsIgnoreCase(name)).findFirst();
	}

	public Collection<Database> getAllDatabases() {
		return _databases;
	}

	public void addDatabase(Database db) {
		_databases.add(db);
	}
}
