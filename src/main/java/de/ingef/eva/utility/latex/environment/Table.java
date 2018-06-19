package de.ingef.eva.utility.latex.environment;

import de.ingef.eva.utility.latex.alignment.Anchor;
import de.ingef.eva.utility.latex.table.Tabular;
import lombok.Getter;

/**
 * Short-hand for latex table environment
 * 
 * @author Martin.Wettig
 *
 */
@Getter
public class Table extends AlignedEnvironment {
	
	public Table(Anchor anchor, Tabular child) {
		super("table", anchor, child);
	}
	
	@Override
	protected String begin() {
		return String.format("\\begin{table}[%s]\n", position.getPosition());
	}
}
