package de.ingef.eva.configuration;

import java.util.Collection;

public class DatabaseEntry {
	private String _databaseName;
	private String _fetchQuery;
	private Collection<String> _tables;
	
	public DatabaseEntry(String databaseName, String fetchQuery, Collection<String> tables)
	{
		_databaseName = databaseName;
		_tables = tables;
		_fetchQuery = fetchQuery;
	}

	public String getName()
	{
		return _databaseName;
	}
	
	public Collection<String> getTables()
	{
		return _tables;
	}
	
	public String getFetchQuery()
	{
		return _fetchQuery;
	}
}
