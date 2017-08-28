package de.ingef.eva.query.creation;

import java.util.ArrayList;
import java.util.Collection;

import de.ingef.eva.database.Column;
import de.ingef.eva.query.Query;

public class SimpleQuery implements Query {

	private String _name;
	private String _query;
	private Collection<Column> _columns = new ArrayList<Column>(10);

	public SimpleQuery(String query, Collection<Column> columns) {
		_query = query;
		_columns = columns;
	}

	@Override
	public String getName() {
		return _name;
	}

	@Override
	public Collection<Column> getSelectedColumns() {
		return _columns;
	}

	@Override
	public String getQuery() {
		return _query;
	}

	@Override
	public void setName(String name) {
		_name = name;
	}
}
