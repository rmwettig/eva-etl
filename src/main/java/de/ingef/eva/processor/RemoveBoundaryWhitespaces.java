package de.ingef.eva.processor;

public class RemoveBoundaryWhitespaces implements Processor<StringBuilder> 
{
	public StringBuilder process(StringBuilder value)
	{
		int index = 0;
		while(value.charAt(index) == ' ')
		{
			index++;
		}
		value.delete(0, index);
		index = value.length()-1;
		while(value.charAt(index) == ' ')
		{
			index--;
		}
		value.delete(index+1, value.length());
		
		
		return value;
	}
}
