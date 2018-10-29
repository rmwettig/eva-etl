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

	protected abstract List<String> prepareConditionValues();
}
