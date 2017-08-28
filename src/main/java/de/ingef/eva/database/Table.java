package de.ingef.eva.database;

import java.util.List;

public interface Table {
	void addColumn(Column c);

	Column findColumnByName(String name);

	List<Column> getAllColumns();

	String getName();
}
