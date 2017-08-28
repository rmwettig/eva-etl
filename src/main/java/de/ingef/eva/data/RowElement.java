package de.ingef.eva.data;

public interface RowElement {
	public int getIndex();
	public String getName();
	public String getContent();
	public TeradataColumnType getType();
}
