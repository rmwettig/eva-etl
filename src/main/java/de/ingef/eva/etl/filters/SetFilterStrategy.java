package de.ingef.eva.etl.filters;

import java.util.Collections;
import java.util.Set;

import com.fasterxml.jackson.annotation.JsonTypeName;

import de.ingef.eva.configuration.FilterStrategyType;

@JsonTypeName(value="UNIQUE_SET")
public class SetFilterStrategy extends FilterStrategy {

	private Set<String> values;
	
	public SetFilterStrategy() {
		this(Collections.emptySet());
	}
	
	public SetFilterStrategy(Set<String> values) {
		super(FilterStrategyType.UNIQUE_SET);
		this.values = values;
	}

	@Override
	public boolean isValid(String value) {
		return values.contains(value);
	}

}
