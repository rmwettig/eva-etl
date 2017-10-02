package de.ingef.eva.query;

import java.util.Collection;

import de.ingef.eva.database.Column;

public interface Query {
	String getName();
	void setName(String name);
	Collection<Column> getSelectedColumns();
	String getQuery();
	String getDbName();
	String getTableName();
	String getSliceName();
	String getDatasetName();
	void setDatasetName(String dataset);
}
