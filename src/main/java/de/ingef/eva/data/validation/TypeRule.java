package de.ingef.eva.data.validation;

import de.ingef.eva.data.RowElement;
import de.ingef.eva.data.SimpleRowElement;
import de.ingef.eva.data.TeradataColumnType;
import lombok.RequiredArgsConstructor;
/**
 * A TypeRule validates the row element content based on the intended type of the row element content
 * 
 * @author Martin Wettig
 *
 */
@RequiredArgsConstructor
public class TypeRule implements Rule {
	
	private final TeradataColumnType type;
	private final ReplacePattern validation;
	
	@Override
	public RowElement validate(RowElement rowElement) {
		if (type != TeradataColumnType.ANY && rowElement.getType() != type) return rowElement;
		String cleanContent = validation.process(rowElement.getContent());
		return new SimpleRowElement(rowElement.getName(), rowElement.getIndex(), rowElement.getType(), cleanContent);
	}
}
