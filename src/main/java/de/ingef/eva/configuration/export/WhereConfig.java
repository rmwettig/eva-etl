package de.ingef.eva.configuration.export;

import de.ingef.eva.configuration.WhereSource;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;

@Getter
@ToString
@AllArgsConstructor
public class WhereConfig {
	private String column;
	private String value;
	private WhereOperator operator;
	private WhereType type;
	private WhereSource source;
}
