package de.ingef.eva.configuration.export.sql;

import com.fasterxml.jackson.annotation.JsonTypeName;

import lombok.Getter;
import lombok.Setter;

@Getter @Setter
@JsonTypeName(value="COLUMN")
public class ColumnNode extends SqlNode {

	private final String name;
	private final String alias;
	
	public ColumnNode(String columnName) {
		this(columnName, "");
	}
	
	public ColumnNode(String columnName, String columnAlias) {
		super(SqlNodeType.COLUMN);
		name = columnName;
		alias = columnAlias;
	}
}
