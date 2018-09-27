package de.ingef.eva.utility.latex.table;

/**
 * Horizontal line for latex table
 * 
 * @author Martin.Wettig
 *
 */
public class HLine extends TableRow {

	@Override
	protected String renderRow() {
		return "\\hline";
	}

}
