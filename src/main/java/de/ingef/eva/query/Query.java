package de.ingef.eva.query;

import java.util.Collection;

import de.ingef.eva.database.Column;

public interface Query {
	String getName();
	void setName(String name);
	Collection<Column> getSelectedColumns();
	String getQuery();
	String getDbName();
	void setDbName(String name);
	String getTableName();
	void setTableName(String name);
	String getSliceName();
	void setSliceName(String name);
	String getDatasetName();
	void setDatasetName(String name);
}
