package de.ingef.eva.database;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
@Getter
public class TextColumn implements Column {

	private final String name;
	private final String type;

	public TextColumn(String name) {
		this(name, "");
	}
}
