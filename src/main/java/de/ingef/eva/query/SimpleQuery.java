package de.ingef.eva.query;

import java.util.ArrayList;
import java.util.Collection;

public class SimpleQuery implements Query {

	private String _name;
	private String _query;
	private Collection<String> _columns = new ArrayList<String>(10);

	public SimpleQuery(String query, Collection<String> columns) {
		_query = query;
		_columns = columns;
	}

	@Override
	public String getName() {
		return _name;
	}

	@Override
	public Collection<String> getSelectedColumns() {
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
