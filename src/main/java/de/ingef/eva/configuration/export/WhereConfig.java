package de.ingef.eva.configuration.export;

import java.util.List;

import de.ingef.eva.configuration.export.sql.SqlNode;
import de.ingef.eva.configuration.export.sql.SqlNodeType;
import de.ingef.eva.query.creation.QueryCreator;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

/**
 * Model class for json where condition definition
 * @author Martin.Wettig
 *
 */
@Getter @Setter
@ToString
public abstract class WhereConfig extends SqlNode {
	/**
	 * Name of the restricted column
	 */
	private String column;
	/**
	 * @deprecated use values field instead. {@code values} allows to specify multiple values for a column at once.
	 */
	private String value;
	/**
	 * Operator to be used for comparison
	 */
	private WhereOperator operator;
	/**
	 * Type that is compared
	 */
	private WhereType columnType;
	
	public WhereConfig(SqlNodeType whereType) {
		super(whereType);
	}
	
	/**
	 * @deprecated unused
	 * @param whereType
	 * @param columnName
	 * @param valueContent
	 * @param op
	 * @param type
	 */
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
