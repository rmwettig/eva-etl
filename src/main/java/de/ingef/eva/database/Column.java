package de.ingef.eva.database;

import de.ingef.eva.data.TeradataColumnType;

public interface Column {
	String getName();

	TeradataColumnType getType();
}
