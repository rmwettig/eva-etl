package de.ingef.eva.configuration.export.sql;

public enum SqlNodeType {
	COLUMN,
	TABLE,
	DB,
	JOIN,
	WHERE_INLINE,
	WHERE_FILE,
	FIXED_YEAR_SLICE,
	DYNAMIC_YEAR_SLICE
}
