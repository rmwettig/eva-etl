package de.ingef.eva.data;

import lombok.Getter;
import lombok.ToString;

@Getter
@ToString(of= {"type", "content"})
public class SimpleRowElement implements RowElement {
	
	private String name;
	private int index;
	private TeradataColumnType type;
	private String content;
	
	@Deprecated
	public SimpleRowElement(String name, int index, TeradataColumnType type, String content) {
		this.name = name;
		this.index = index;
		this.type = type;
		this.content = content;
	}
	
	public SimpleRowElement(String content, TeradataColumnType type) {
		this.content = content;
		this.type = type;
	}
}
