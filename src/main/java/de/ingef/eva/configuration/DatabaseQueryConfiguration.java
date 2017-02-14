package de.ingef.eva.configuration;

import java.util.Collection;
import java.util.Map;


/**
 * Encapsulates database specific settings like views and year interval
 * @author Martin Wettig
 *
 */
public class DatabaseQueryConfiguration {
	
	private int startYear;
	private int endYear;
	private Map<String, Collection<String>> databaseViews;
	
	public DatabaseQueryConfiguration(int startYear, int endYear, Map<String, Collection<String>> views)
	{
		this.startYear = startYear;
		this.endYear = endYear;
		databaseViews = views;
	}
	
	public Collection<String> getDatabaseNames()
	{
		return databaseViews.keySet();
	}
	
	public Collection<String> getViews(String databaseName)
	{
		return databaseViews.get(databaseName);
	}

	public int getStartYear() {
		return startYear;
	}

	public void setStartYear(int startYear) {
		this.startYear = startYear;
	}

	public int getEndYear() {
		return endYear;
	}

	public void setEndYear(int endYear) {
		this.endYear = endYear;
	}
}
