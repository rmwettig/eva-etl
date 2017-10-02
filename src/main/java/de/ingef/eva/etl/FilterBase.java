package de.ingef.eva.etl;

public abstract class FilterBase {
	public abstract boolean isValid(Row row);
}
