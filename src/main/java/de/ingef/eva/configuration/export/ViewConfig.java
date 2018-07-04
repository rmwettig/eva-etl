package de.ingef.eva.configuration.export;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonTypeName;

import de.ingef.eva.configuration.export.sql.ColumnNode;
import de.ingef.eva.configuration.export.sql.SqlNode;
import de.ingef.eva.configuration.export.sql.SqlNodeType;
import de.ingef.eva.query.Query;
import de.ingef.eva.query.creation.QueryCreator;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

/**
 * Model class for a table from which data is exported
 * @author Martin.Wettig
 *
 */
@Getter @Setter
@JsonTypeName(value="TABLE")
@ToString
public class ViewConfig extends SqlNode {
	/**
	 * Name of the table
	 */
	private String name;
	/**
	 * Columns that should be exported
	 */
	private List<ColumnNode> columns;
	/**
	 * Names of columns that should be ignored
	 */
	private List<String> excludeColumns;
	/**
	 * Conditions for columns in this table
	 */
	private List<WhereConfig> where;
	/**
	 * Joins to add data to this table
	 */
	private List<JoinConfig> joins;
		
	public ViewConfig() {
		super(SqlNodeType.TABLE);
	}
	
	public List<Query> traverse(SourceConfig parent, QueryCreator builder, List<WhereConfig> globalConditions) {
		builder.addTable(name);
		processColumns(builder);
		traverseJoins(builder);
		traverseConditions(builder);
		traverseGlobalConditions(builder, globalConditions);
		return builder.buildQueries();
	}

	private void traverseJoins(QueryCreator builder) {
		if(joins == null || joins.isEmpty()) return;
		
		joins.stream().forEach(join -> join.traverse(this, builder));
	}
	
	private void traverseConditions(QueryCreator builder) {
		if(where == null || where.isEmpty()) return;
		
		where.stream().forEach(condition -> condition.traverse(name, builder));
	}
	
	private void processColumns(QueryCreator builder) {
		if(columns == null || columns.isEmpty())
			if(excludeColumns == null || excludeColumns.isEmpty())
				builder.addAllKnownColumns(name);
			else
				builder.addAllKnownColumns(name, excludeColumns);
		else
			columns.stream().forEach(column -> builder.addColumn(name, column.getName()));
	}
	
	private void traverseGlobalConditions(QueryCreator builder, List<WhereConfig> globalConditions) {
		if(globalConditions.isEmpty()) return;
		
		globalConditions.stream().forEach(condition -> condition.traverse(name, builder));
	}
}
