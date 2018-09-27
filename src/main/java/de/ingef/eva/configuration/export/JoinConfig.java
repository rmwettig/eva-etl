package de.ingef.eva.configuration.export;

import java.util.List;
import java.util.stream.Collectors;

import com.fasterxml.jackson.annotation.JsonTypeName;

import de.ingef.eva.configuration.export.sql.ColumnNode;
import de.ingef.eva.configuration.export.sql.SqlNode;
import de.ingef.eva.configuration.export.sql.SqlNodeType;
import de.ingef.eva.query.creation.QueryCreator;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

/**
 * Model class for json join definition
 * @author Martin.Wettig
 *
 */
@Getter @Setter
@JsonTypeName(value="JOIN")
@ToString
public class JoinConfig extends SqlNode {
	/**
	 * Name of the table that is used as an additional data source
	 */
	private String name;
	/**
	 * Columns that are included from the joined table.
	 * If this field is missing or empty all known columns are used.
	 */
	private List<ColumnNode> columns;
	/**
	 * Columns that should be ignored
	 */
	private List<String> excludeColumns;
	/**
	 * Type of the join used
	 */
	private JoinType joinType;
	/**
	 * Columns that are used to connect the two tables.
	 * All columns in this list are compared for equality and are logically AND-ed
	 */
	private List<ColumnNode> onColumns;
	/**
	 * Additional conditions on the join table columns
	 */
	private List<WhereConfig> where;
	
	public JoinConfig() {
		super(SqlNodeType.JOIN);
	}
	
	public void traverse(ViewConfig parent, QueryCreator builder) {
		processColumns(builder);
		processJoinColumns(parent, builder);
		processWheres(builder);
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

	private void processWheres(QueryCreator builder) {
		if(where == null || where.isEmpty()) return;
		where.stream().forEach(w -> w.traverse(name, builder));
	}

	private void processJoinColumns(ViewConfig parent, QueryCreator builder) {
		List<String> columns =
				onColumns
					.stream()
					.map(column -> column.getName())
					.collect(Collectors.toList());
		builder.addJoin(parent.getName(), name, columns, joinType);
	}
}
