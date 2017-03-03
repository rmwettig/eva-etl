package de.ingef.eva.database;

import java.util.Collection;

public interface DatabaseHost {
	Database findDatabaseByName(String name);
	Collection<Database> getAllDatabases();
}
