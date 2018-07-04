package de.ingef.eva.database;

import de.ingef.eva.data.TeradataColumnType;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

@RequiredArgsConstructor
@Getter
@EqualsAndHashCode
@ToString
public class Column {

	private final String name;
	private final TeradataColumnType type;

	public Column(String name) {
		this(name, TeradataColumnType.UNKNOWN);
	}
}
