package de.ingef.eva.query;

public interface Query {
	String getName();
	void setName(String name);
	String getQuery();
	String getDbName();
	void setDbName(String name);
	String getTableName();
	void setTableName(String name);
	String getSliceName();
	void setSliceName(String name);
	String getDatasetName();
	void setDatasetName(String name);
	String getDescription();
}
