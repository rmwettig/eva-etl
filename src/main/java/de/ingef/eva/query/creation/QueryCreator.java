package de.ingef.eva.query.creation;

import java.util.List;

import de.ingef.eva.configuration.export.JoinType;
import de.ingef.eva.configuration.export.WhereOperator;
import de.ingef.eva.configuration.export.WhereType;
import de.ingef.eva.configuration.export.sql.YearSliceNode;
import de.ingef.eva.query.Query;
import de.ingef.eva.utility.Alias;

public interface QueryCreator {
	public void setDatabase(String name);

	public void addTable(String name);

	public void addColumn(String table, String name);

	public void addAllKnownColumns(String table);
	
	public void addAllKnownColumns(String table, List<String> excludeColumns);
	
	public void addJoin(String leftTable, String rightTable, List<String> onColumns, JoinType type);

	public void addWhere(String table, String column, List<String> value, WhereOperator operator, WhereType type);
	
	public void startOrGroup();

	public void endOrGroup();

	public List<Query> buildQueries();
	
	public void setYearSlice(YearSliceNode slice);
	
	public void setDatasetName(String datasetName);

	public void addGlobalWhere(String table, String column, List<String> values, WhereOperator symbol, WhereType name);
}
