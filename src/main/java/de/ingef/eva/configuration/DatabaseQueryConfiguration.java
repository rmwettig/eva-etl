package de.ingef.eva.configuration;

import java.util.Collection;


/**
 * Encapsulates database specific settings like views and year interval
 * @author Martin Wettig
 *
 */
public class DatabaseQueryConfiguration {
	
	private int _startYear;
	private int _endYear;
	private Collection<DatabaseEntry> _entries;
	
	public DatabaseQueryConfiguration(int startYear, int endYear, Collection<DatabaseEntry> entries)
	{
		_startYear = startYear;
		_endYear = endYear;
		_entries = entries;
	}
	
	public Collection<DatabaseEntry> getEntries() {
		return _entries;
	}

	public int getStartYear() {
		return _startYear;
	}

	public void setStartYear(int startYear) {
		this._startYear = startYear;
	}

	public int getEndYear() {
		return _endYear;
	}

	public void setEndYear(int endYear) {
		this._endYear = endYear;
	}
}
