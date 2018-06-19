package de.ingef.eva.utility.latex.table;

import de.ingef.eva.utility.latex.LatexNode;

/**
 * Base class of latex table row entries
 * 
 * @author Martin.Wettig
 *
 */
public abstract class TableRow implements LatexNode {
	
	@Override
	public String render() {
		return renderRow();
	}
	
	protected abstract String renderRow();
}
