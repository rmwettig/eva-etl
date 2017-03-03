package de.ingef.eva.database;

import java.util.ArrayList;
import java.util.Collection;

public class TextDatabase implements Database {

	private Collection<Table> _tables = new ArrayList<Table>();
	private String _name;
	
	public TextDatabase(String name){
		_name = name;
	}
	
	@Override
	public Table findTableByName(String name) {
		for(Table t : _tables)
		{
			if(t.getName().equalsIgnoreCase(name))
				return t;
		}
		return null;
	}

	@Override
	public Collection<Table> getAllTables() {
		return _tables;
	}

	@Override
	public String getName() {
		return _name;
	}

	@Override
	public void addTable(Table t) {
		_tables.add(t);
	}

}
