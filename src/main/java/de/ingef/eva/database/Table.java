package de.ingef.eva.database;

import java.util.Collection;

public interface Table {
	void addColumn(Column c);
	Column findColumnByName(String name);
	Collection<Column> getAllColumns();
	String getName();
}
