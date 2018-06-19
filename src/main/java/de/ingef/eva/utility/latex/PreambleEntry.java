package de.ingef.eva.utility.latex;

import java.util.List;
import java.util.stream.Collectors;

import lombok.Builder;
import lombok.Getter;
import lombok.Singular;

/**
 * Document preamble setting
 * 
 * @author Martin.Wettig
 *
 */
@Getter
@Builder
public class PreambleEntry implements LatexNode {
	
	private String name;
	@Singular
	private List<String> options;
	private String value;
	
	@Override
	public String render() {
		if(options.isEmpty())
			return renderNoOptions();
		else
			return renderWithOptions();
	}
	
	private String renderNoOptions() {
		return String.format(
				"\\%s{%s}",
				name,
				value
			);
	}
	
	private String renderWithOptions() {
		return String.format(
			"\\%s[%s]{%s}",
			name,
			options.stream().collect(Collectors.joining(", ")),
			value
		);
	}
}
