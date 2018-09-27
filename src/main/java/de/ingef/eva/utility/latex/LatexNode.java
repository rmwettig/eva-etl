package de.ingef.eva.utility.latex;

/**
 * Any latex commands must implement implement this to be able to create a textual representation
 * 
 * @author Martin.Wettig
 *
 */
public interface LatexNode {
	public String render();
}
