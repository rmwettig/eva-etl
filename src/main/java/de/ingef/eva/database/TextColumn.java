package de.ingef.eva.database;

import de.ingef.eva.data.TeradataColumnType;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
@Getter
public class TextColumn implements Column {

	private final String name;
	private final TeradataColumnType type;

	public TextColumn(String name) {
		this(name, TeradataColumnType.UNKNOWN);
	}
}
