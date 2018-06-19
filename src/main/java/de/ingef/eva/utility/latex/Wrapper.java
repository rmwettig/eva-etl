package de.ingef.eva.utility.latex;

import lombok.RequiredArgsConstructor;

/**
 * Enclose latex node within textual symbols, e.g. brackets
 * 
 * @author Martin.Wettig
 *
 */
@RequiredArgsConstructor
public class Wrapper implements LatexNode {

	private final String openSymbol;
	private final String endSymbol;
	private final LatexNode child;
	
	@Override
	public String render() {
		return openSymbol + child.render() + endSymbol;
	}

}
