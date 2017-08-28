package de.ingef.eva.data.validation;

import de.ingef.eva.data.RowElement;

public interface Rule {
	RowElement validate(RowElement rowElement);
}
