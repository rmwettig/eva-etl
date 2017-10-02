package de.ingef.eva.etl;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

@Getter
@RequiredArgsConstructor
public abstract class Filter {
	private final String name;
	public abstract boolean isValid(Row row);
}
