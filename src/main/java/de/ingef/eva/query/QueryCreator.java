package de.ingef.eva.query;

import de.ingef.eva.utility.Alias;

public interface QueryCreator {
	public void setDatabase(String name);

	public void addTable(String name);

	public void addColumn(String table, String name);

	public void addJoin(String leftTable, String rightTable, String onColumn, String type);

	public void addWhere(String table, String column, String value, String operator, String type);

	public void addWhere(String leftTable, String leftColumn, String rightTable, String rightColumn, String operator, String type);
	
	public void startOrGroup();

	public void endOrGroup(String table);

	public void setAliasFactory(Alias alias);

	public Query buildQuery();
}
