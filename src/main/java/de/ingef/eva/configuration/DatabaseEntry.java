package de.ingef.eva.configuration;

import java.util.Collection;

public class DatabaseEntry {
	private String _databaseName;
	private Collection<String> _tables;
	
	public DatabaseEntry(String databaseName, Collection<String> tables)
	{
		_databaseName = databaseName;
		_tables = tables;
	}

	public String getName()
	{
		return _databaseName;
	}
	
	public Collection<String> getTables()
	{
		return _tables;
	}
}
