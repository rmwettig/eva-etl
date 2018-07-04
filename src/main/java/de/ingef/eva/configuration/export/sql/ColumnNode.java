package de.ingef.eva.configuration.export.sql;

import com.fasterxml.jackson.annotation.JsonTypeName;

import lombok.Getter;
import lombok.Setter;

/**
 * Model class for json column definition
 * @author Martin.Wettig
 *
 */
@Getter @Setter
@JsonTypeName(value="COLUMN")
public class ColumnNode extends SqlNode {

	/**
	 * Name of the column
	 */
	private final String name;
	/**
	 * @deprecated unused
	 */
	private final String alias;
	
	public ColumnNode(String columnName) {
		this(columnName, "");
	}
	
	/**
	 * @deprecated unused
	 * @param columnName
	 * @param columnAlias
	 */
	public ColumnNode(String columnName, String columnAlias) {
		super(SqlNodeType.COLUMN);
		name = columnName;
		alias = columnAlias;
	}
}
