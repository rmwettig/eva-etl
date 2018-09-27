package de.ingef.eva.configuration.export;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public enum WhereOperator {
	EQUAL("="),
	LESS("<"),
	LARGER(">"), 
	LIKE("like");
	
	private String symbol;
	
}
