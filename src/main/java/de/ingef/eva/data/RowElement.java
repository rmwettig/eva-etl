package de.ingef.eva.data;

public interface RowElement {

	@Deprecated
	public int getIndex();
	@Deprecated
	public String getName();
	public String getContent();
	public TeradataColumnType getType();
}
