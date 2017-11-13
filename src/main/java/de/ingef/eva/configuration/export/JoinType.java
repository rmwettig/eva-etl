package de.ingef.eva.configuration.export;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

@Getter
@RequiredArgsConstructor
public enum JoinType {
	INNER("inner"),
	LEFT("left");
	
	private final String name;
}
