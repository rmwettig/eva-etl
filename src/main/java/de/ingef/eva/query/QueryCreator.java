package de.ingef.eva.query;

public interface QueryCreator {
	public void setDatabase(String name);
	public void addTable(String name);
	public void addColumn(String table, String name);
	public void addJoin(String leftTable, String rightTable, String onColumn, String type);
	public void addWhere(String table, String column, String value, String operator, String type);
	public void startOrGroup();
	public void endOrGroup(String table);
	public Query buildQuery();
}
