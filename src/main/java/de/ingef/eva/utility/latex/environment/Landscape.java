package de.ingef.eva.utility.latex.environment;

import de.ingef.eva.utility.latex.LatexNode;

/**
 * Short-hand class for landscape environment
 * 
 * @author Martin.Wettig
 *
 */
public class Landscape extends Environment {
	
	public Landscape(LatexNode child) {
		super("landscape", child);
	}
}