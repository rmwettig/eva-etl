package de.ingef.eva.database;

import java.util.Collection;

public interface Database {
	void addTable(Table t);
	Table findTableByName(String name);
	Collection<Table> getAllTables();
	String getName();
}
