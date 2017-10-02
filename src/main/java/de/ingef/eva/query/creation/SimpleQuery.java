package de.ingef.eva.query.creation;

import java.util.ArrayList;
import java.util.Collection;

import de.ingef.eva.database.Column;
import de.ingef.eva.query.Query;
import lombok.Getter;
import lombok.Setter;

@Getter
public class SimpleQuery implements Query {

	@Setter
	private String name;
	private String query;
	@Setter
	private String dbName;
	@Setter
	private String tableName;
	@Setter
	private String sliceName;
	@Setter
	private String datasetName;
	private Collection<Column> columns = new ArrayList<Column>(10);

	public SimpleQuery(String query, Collection<Column> columns) {
		this(query, columns, "", "", "", "");
	}
	
	public SimpleQuery(String query, Collection<Column> columns, String dbName, String tableName, String sliceName, String datasetName) {
		this.query = query;
		this.columns = columns;
		this.dbName = dbName;
		this.tableName = tableName;
		this.sliceName = sliceName;
		this.datasetName = datasetName;
	}

	@Override
	public Collection<Column> getSelectedColumns() {
		return columns;
	}
}
