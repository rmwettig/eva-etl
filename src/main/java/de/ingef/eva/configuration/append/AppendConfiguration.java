package de.ingef.eva.configuration.append;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

@Getter
@RequiredArgsConstructor
public class AppendConfiguration {
	private final AppendMode mode;
	private final AppendOrder order;
	private final String targets;
	private final String match;
	private final String targetColumn;
	private final String value;
	private final String source;
	private final String keyColumn;
}