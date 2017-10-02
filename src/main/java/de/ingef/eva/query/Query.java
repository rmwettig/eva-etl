package de.ingef.eva.query;

import java.util.Collection;

import de.ingef.eva.database.Column;

public interface Query {
	String getName();
	void setName(String name);
	Collection<Column> getSelectedColumns();
	String getQuery();
	String getDBName();
	String getTableName();
	String getSliceName();
	String getDatasetLabel();
	void setDatasetLabel(String dataset);
}
