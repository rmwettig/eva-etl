package de.ingef.eva.utility.latex.table;

import de.ingef.eva.utility.latex.alignment.Anchor;
import lombok.RequiredArgsConstructor;

/**
 * Latex table row that spans multiple columns
 * 
 * @author Martin.Wettig
 *
 */
@RequiredArgsConstructor
public class MultiColumnRow extends TableRow {

	private final int columnSpan;
	private final String value;
	private final Anchor position;
	
	@Override
	protected String renderRow() {
		return String.format("\\multicolumn{%d}{%s}{%s}\\\\", columnSpan, position.getPosition(), value);
	}

}
