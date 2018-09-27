package de.ingef.eva.utility.latex.table;

import java.util.List;
import java.util.stream.Collectors;

import lombok.Builder;
import lombok.Singular;

/**
 * Values of a latex table
 * 
 * @author Martin.Wettig
 *
 */
@Builder
public class RowContent extends TableRow {

	@Singular
	private List<String> values;

	@Override
	protected String renderRow() {
		return values.stream().collect(Collectors.joining(" & ")) + "\\\\";
	}

}
