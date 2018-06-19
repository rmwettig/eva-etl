package de.ingef.eva.utility.latex.environment;

import de.ingef.eva.utility.latex.LatexNode;
import de.ingef.eva.utility.latex.alignment.Anchor;

/**
 * Environment that supports a single alignment option
 * 
 * @author Martin.Wettig
 *
 */
public class AlignedEnvironment extends Environment {
	
	protected final Anchor position;
	
	public AlignedEnvironment(String name, Anchor anchor, LatexNode child) {
		super(name, child);
		position = anchor;
	}
	
	@Override
	protected String begin() {
		return String.format("\\begin{%s}[%s]\n", name, position.getPosition()); 
	}
}