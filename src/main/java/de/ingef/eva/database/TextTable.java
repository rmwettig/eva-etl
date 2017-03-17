package de.ingef.eva.database;

import java.util.ArrayList;
import java.util.Collection;

public class TextTable implements Table {

	private String _name = "";
	private Collection<Column> _columns = new ArrayList<Column>();

	public TextTable(String name) {
		_name = name;
	}

	@Override
	public void addColumn(Column c) {
		_columns.add(c);
	}

	@Override
	public Column findColumnByName(String name) {
		for (Column c : _columns) {
			if (c.getName().equalsIgnoreCase(name))
				return c;
		}
		return null;
	}

	@Override
	public Collection<Column> getAllColumns() {
		return _columns;
	}

	@Override
	public String getName() {
		return _name;
	}

}
