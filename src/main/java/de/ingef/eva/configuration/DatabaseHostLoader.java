package de.ingef.eva.configuration;

import de.ingef.eva.database.DatabaseHost;

public interface DatabaseHostLoader {
	DatabaseHost loadFromFile(String file);
}
