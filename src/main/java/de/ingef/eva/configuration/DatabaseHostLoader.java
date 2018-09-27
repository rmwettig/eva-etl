package de.ingef.eva.configuration;

import de.ingef.eva.database.DatabaseSchema;

public interface DatabaseHostLoader {
	DatabaseSchema loadFromFile(String file);
}
