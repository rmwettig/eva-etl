package de.ingef.eva.data;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
@Getter
public class SimpleRowElement implements RowElement {
	
	private final String name;
	private final int index;
	private final TeradataColumnType type;
	private final String content;
	
}
