package de.ingef.eva.configuration.export;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonTypeName;

import de.ingef.eva.configuration.export.sql.SqlNodeType;
import lombok.Getter;
import lombok.Setter;

/**
 * Model class for a where condition that has its values within the json file
 * @author Martin.Wettig
 *
 */
@JsonTypeName(value="WHERE_INLINE")
@Getter @Setter
public class InlineCondition extends WhereConfig {

	private List<String> values;
	
	public InlineCondition() {
		super(SqlNodeType.WHERE_INLINE);
	}
	
	public InlineCondition(String columnName, List<String> values, WhereOperator op, WhereType type) {
		super(SqlNodeType.WHERE_INLINE, columnName, op, type);
		this.values = values;
	}
	
	@Override
	protected List<String> prepareConditionValues() {
		return values;
	}

}
