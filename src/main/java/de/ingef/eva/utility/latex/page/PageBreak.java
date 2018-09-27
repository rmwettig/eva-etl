package de.ingef.eva.utility.latex.page;

import de.ingef.eva.utility.latex.LatexNode;

/**
 * PageBreak creates a new page in the produced pdf
 * 
 * @author Martin.Wettig
 *
 */
public class PageBreak implements LatexNode {

	@Override
	public String render() {
		return "\\newpage";
	}

}
