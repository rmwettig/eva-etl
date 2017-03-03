package de.ingef.eva.configuration;

import java.util.Collection;
import java.util.Map;


/**
 * Holds specific information about the database and included tables
 * along with conditions that should be applied to all queries.
 * 
 * @author Martin Wettig
 *
 */
public class DatabaseEntry {
	private String _databaseName;
	private Collection<String> _tables;
	private String _globalCondition;
	public DatabaseEntry(String databaseName, Collection<String> tables, Map<String, Collection<String>> globalConditions)
	{
		_databaseName = databaseName;
		_tables = tables;
		_globalCondition = (globalConditions != null)? combineIntoString(globalConditions) : "";
	}

	public String getName()
	{
		return _databaseName;
	}
	
	public Collection<String> getTables()
	{
		return _tables;
	}
	
	public String getCondition()
	{
		return _globalCondition;
	}
	
	private String combineIntoString(Map<String, Collection<String>> conditions)
	{
		StringBuilder globalCondition = new StringBuilder();
		String format = "%s='%s'";
		for(String column : conditions.keySet())
		{
			StringBuilder orConditions = new StringBuilder();
			orConditions.append("(");
			for(String value : conditions.get(column))
			{
				orConditions.append(String.format(format, column, value));
				orConditions.append(" or ");
			}
			orConditions.delete(orConditions.lastIndexOf(" or "), orConditions.length());
			orConditions.append(")");
			globalCondition.append(orConditions);
			globalCondition.append(" and ");
		}
		//globalCondition.delete(globalCondition.lastIndexOf(" and "), globalCondition.length());
		
		return globalCondition.toString();
	}
}
