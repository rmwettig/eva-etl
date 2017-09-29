package de.ingef.eva.query.creation;

import java.util.ArrayList;
import java.util.Collection;

import de.ingef.eva.database.Column;
import de.ingef.eva.query.Query;

public class SimpleQuery implements Query {

	private String _name;
	private String _query;
	private String _dbName;
	private String _tableName;
	private String _sliceName;
	private Collection<Column> _columns = new ArrayList<Column>(10);

	public SimpleQuery(String query, Collection<Column> columns) {
		this(query, columns, "", "", "");
	}
	
	public SimpleQuery(String query, Collection<Column> columns, String dbName, String tableName, String sliceName) {
		_query = query;
		_columns = columns;
		_dbName = dbName;
		_tableName = tableName;
		_sliceName = sliceName;
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

	@Override
	public String getDBName() {
		return _dbName;
	}

	@Override
	public String getTableName() {
		return _tableName;
	}

	@Override
	public String getSliceName() {
		return _sliceName;
	}
}
