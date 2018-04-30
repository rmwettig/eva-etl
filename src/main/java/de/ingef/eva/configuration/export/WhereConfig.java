package de.ingef.eva.configuration.export;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeName;

import de.ingef.eva.configuration.WhereSource;
import de.ingef.eva.configuration.export.sql.SqlNode;
import de.ingef.eva.configuration.export.sql.SqlNodeType;
import de.ingef.eva.query.creation.QueryCreator;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Getter @Setter
@ToString
public abstract class WhereConfig extends SqlNode {
	private String column;
	/**
	 * @deprecated use values field instead. {@code values} allows to specify multiple values for a column at once.
	 */
	private String value;
	private WhereOperator operator;
	private WhereType columnType;
	
	public WhereConfig(SqlNodeType whereType) {
		super(whereType);
	}
	
	public WhereConfig(SqlNodeType whereType, String columnName, String valueContent, WhereOperator op, WhereType type) {
		super(whereType);
		column = columnName;
		value = valueContent;
		operator = op;
		columnType = type;
	}
	
	public WhereConfig(SqlNodeType whereType, String columnName, WhereOperator op, WhereType type) {
		super(whereType);
		column = columnName;
		operator = op;
		columnType = type;
	}
	
	public void traverse(String table, QueryCreator builder) {
		builder.startOrGroup();
		builder.addWhere(table, column, prepareConditionValues(), operator, columnType);
		builder.endOrGroup();
	}
	
	public void traverseGlobalCondition(String table, QueryCreator builder) {
		builder.addGlobalWhere(table, column, prepareConditionValues(), operator, columnType);
	}
	
	protected abstract List<String> prepareConditionValues();
}
