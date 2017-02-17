package de.ingef.eva.processor;

public class RemovePattern implements Processor<String> {

	private String _regex;
	
	public RemovePattern(String regex)
	{
		_regex = regex;
	}


	@Override
	public String process(String value) 
	{
		return value.replaceAll(_regex, "");
	}

}
