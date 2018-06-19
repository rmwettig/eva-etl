package de.ingef.eva.utility.latex;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

/**
 * Change text color of given value
 * 
 * @author Martin.Wettig
 *
 */
@RequiredArgsConstructor
public class TextColor implements LatexNode {

	private final Color color;
	private final String text;
	
	@Getter
	@RequiredArgsConstructor
	public enum Color {
		RED("red");
		
		private final String name;
	}
	
	@Override
	public String render() {
		return String.format("\\textcolor{%s}{%s}", color.getName(), text);
	}

}
