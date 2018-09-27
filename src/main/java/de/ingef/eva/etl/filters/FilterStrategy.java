package de.ingef.eva.etl.filters;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.annotation.JsonTypeIdResolver;

import de.ingef.eva.configuration.Configuration;
import de.ingef.eva.configuration.FilterStrategyType;
import de.ingef.eva.configuration.export.sql.SqlNodeTypeIdResolver;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

/**
 * Base class for filter behaviour
 * 
 * @author Martin.Wettig
 *
 */
@Getter
@RequiredArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonTypeInfo(use = JsonTypeInfo.Id.CUSTOM, include = JsonTypeInfo.As.PROPERTY, property="type")
@JsonTypeIdResolver(SqlNodeTypeIdResolver.class)
public abstract class FilterStrategy {
	private final FilterStrategyType type;
	public abstract boolean isValid(String value);
	
	/**
	 * initializes strategy subtypes.
	 * This must be called before using {@code isValid}
	 */
	public void initialize(Configuration config) {}
}
