package de.ingef.eva.data.validation;

import de.ingef.eva.data.RowElement;
import de.ingef.eva.data.SimpleRowElement;
import lombok.RequiredArgsConstructor;

/**
 * Validates a RowElement based on the column name it belongs to
 * @author Martin.Wettig
 *
 */
@RequiredArgsConstructor
public class NameRule implements Rule {
	
	private final String columnName;
	private final ReplacePattern validation;
	
	@Override
	public RowElement validate(RowElement rowElement) {
		if(!rowElement.getName().equalsIgnoreCase(columnName)) return rowElement;
		String cleanContent = validation.process(rowElement.getContent());
		return new SimpleRowElement(rowElement.getName(), rowElement.getIndex(), rowElement.getType(), cleanContent);
	}

}
