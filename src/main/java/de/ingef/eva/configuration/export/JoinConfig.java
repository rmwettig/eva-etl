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

@Getter @Setter
@JsonTypeName(value="JOIN")
@ToString
public class JoinConfig extends SqlNode {
	private String table;
	private List<ColumnNode> columns;
	private List<String> excludeColumns;
	private JoinType joinType;
	private List<ColumnNode> onColumns;
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
				builder.addAllKnownColumns(table);
			else
				builder.addAllKnownColumns(table, excludeColumns);
		else
			columns.stream().forEach(column -> builder.addColumn(table, column.getName()));
	}

	private void processWheres(QueryCreator builder) {
		if(where == null || where.isEmpty()) return;
		where.stream().forEach(w -> w.traverse(table, builder));
	}

	private void processJoinColumns(ViewConfig parent, QueryCreator builder) {
		List<String> columns =
				onColumns
					.stream()
					.map(column -> column.getName())
					.collect(Collectors.toList());
		builder.addJoin(parent.getName(), table, columns, joinType);
	}
}
