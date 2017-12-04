package de.ingef.eva.configuration.export;

import lombok.Getter;
import lombok.ToString;

@Getter
@ToString
public class WhereConfig {
	private String column;
	private String value;
	private WhereOperator operator;
	private WhereType type;
}
