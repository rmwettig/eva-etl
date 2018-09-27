package de.ingef.eva.etl.filters;

import java.util.regex.Pattern;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonTypeName;

import de.ingef.eva.configuration.Configuration;
import de.ingef.eva.configuration.FilterStrategyType;
import lombok.Getter;
import lombok.Setter;

/**
 * Uses a regular expression to validate values.
 * An exact match is searched by default.
 * 
 * @author Martin.Wettig
 *
 */
@Getter @Setter
@JsonTypeName(value="PATTERN")
public class RegExFilterStrategy extends FilterStrategy {

	private String regexPattern; 
	@JsonIgnore
	private Pattern regex;
	
	public RegExFilterStrategy() {
		super(FilterStrategyType.PATTERN);
	}
	
	@Override
	public boolean isValid(String value) {
		return regex.matcher(value).matches();
	}

	@Override
	public void initialize(Configuration config) {
		regex = Pattern.compile(regexPattern);
	}
}
