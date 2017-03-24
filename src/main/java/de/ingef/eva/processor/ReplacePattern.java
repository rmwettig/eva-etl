package de.ingef.eva.processor;

public class ReplacePattern implements Processor<String> {

	private String _regex;
	private String _pattern;
	
	public ReplacePattern(String regex, String pattern) {
		_regex = regex;
		_pattern = pattern;
	}

	@Override
	public String process(String value) {
		return value.replaceAll(_regex, _pattern);
	}

}
