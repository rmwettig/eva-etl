package de.ingef.eva.utility.latex;

import lombok.RequiredArgsConstructor;

/**
 * Plain string value
 * 
 * @author Martin.Wettig
 *
 */
@RequiredArgsConstructor
public class Literal implements LatexNode {

	private final String value;
	
	@Override
	public String render() {
		return value;
	}

}
