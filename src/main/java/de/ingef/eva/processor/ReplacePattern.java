package de.ingef.eva.processor;

import de.ingef.eva.constant.TeradataColumnType;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
@Getter
public class ReplacePattern implements Processor<String> {

	private final TeradataColumnType columnType;
	private final String regex;
	private final String pattern;
	
	@Override
	public String process(String value) {
		return value.replaceAll(regex, pattern);
	}

}
