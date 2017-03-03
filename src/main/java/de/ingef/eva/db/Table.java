package de.ingef.eva.db;

import java.util.Collection;

public class Table {
	private String _name;
	private Collection<String> _columns;
	
	public void setName(String name){
		_name = name;
	}
	
	public String getName(){
		return _name;
	}
	
	public void setColumns(Collection<String> columns){
		_columns = columns;
	}
	
	public Collection<String> getColumns(){
		return _columns;
	}
}
