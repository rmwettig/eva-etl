package de.ingef.eva.utility.latex.table;

/**
 * Indicate that a header row of longtable must be repeated on other pages
 * 
 * @author Martin.Wettig
 *
 */
public class EndHead extends TableRow {

	@Override
	protected String renderRow() {
		return "\\endhead";
	}

}
