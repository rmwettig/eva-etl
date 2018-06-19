package de.ingef.eva.utility.latex.environment;

import de.ingef.eva.utility.latex.LatexNode;
import lombok.RequiredArgsConstructor;

/**
 * Base class for environments that enclose other latex objects
 * 
 * @author Martin.Wettig
 *
 */
@RequiredArgsConstructor
public class Environment implements LatexNode {

	/**
	 * Name of the environment
	 */
	protected final String name;
	/**
	 * Enclosed latex object
	 */
	private final LatexNode child;
	
	@Override
	public String render() {
		return new StringBuilder()
					.append(begin())
					.append(child.render())
					.append("\n")
					.append(end())
					.toString();
	}
	
	protected String begin() {
		return String.format("\\begin{%s}\n", name);
	}
	
	protected String end() {
		return String.format("\\end{%s}", name);
	}
}
