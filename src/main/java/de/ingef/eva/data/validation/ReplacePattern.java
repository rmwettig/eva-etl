package de.ingef.eva.data.validation;

import de.ingef.eva.processor.Processor;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
@Getter
public class ReplacePattern implements Processor<String> {
	private final String regex;
	private final String pattern;
	
	@Override
	public String process(String value) {
		return value.replaceAll(regex, pattern);
	}

}
