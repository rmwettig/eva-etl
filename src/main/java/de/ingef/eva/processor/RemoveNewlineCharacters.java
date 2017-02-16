package de.ingef.eva.processor;


/***
 * This processor removes control sequences for newlines
 * @author Martin Wettig
 *
 */
public class RemoveNewlineCharacters implements Processor<StringBuilder> {

	public StringBuilder process(StringBuilder s)
	{
		removeAllCharacterOccurances(s, "\r");
		removeAllCharacterOccurances(s, "\n");
		return s;
	}
	
	private void removeAllCharacterOccurances(StringBuilder sb, String character)
	{
		int characterIndex = sb.indexOf(character);
		while(characterIndex != -1)
		{
			sb.deleteCharAt(characterIndex);
			characterIndex = sb.indexOf(character, characterIndex);
		}
	}

}
