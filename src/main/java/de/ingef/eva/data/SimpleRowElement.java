package de.ingef.eva.data;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
@Getter
public class SimpleRowElement implements RowElement {
	
	private final String content;
	private final TeradataColumnType type;
	
}
