package de.ingef.eva.query;

import java.util.Collection;

public interface Query 
{
	String getName();
	void setName(String name);
	Collection<String> getSelectedColumns();
	String getQuery();
	
}
