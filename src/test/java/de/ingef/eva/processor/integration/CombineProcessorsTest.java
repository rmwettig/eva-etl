package de.ingef.eva.processor.integration;

import static org.junit.Assert.*;

import org.junit.Test;

import de.ingef.eva.processor.RemoveBoundaryWhitespaces;
import de.ingef.eva.processor.RemoveNewlineCharacters;

public class CombineProcessorsTest {

	@Test
	public void testWhitespaceAndNewlineRemoval() 
	{
		RemoveNewlineCharacters p = new RemoveNewlineCharacters();
		RemoveBoundaryWhitespaces p2= new RemoveBoundaryWhitespaces();
		
		//mixed whitespaces and duplicated control sequences//TODO move to integration test
		StringBuilder s5 = new StringBuilder("  \nkey3 \n \n  ");
		StringBuilder s6 = new StringBuilder("\r  value3  \n");
		
//		//even mixed characters must be removed
//		//and potential whitespaces dropped
		assertEquals("key3", p2.process(p.process(s5)).toString());
		assertEquals("value3", p2.process(p.process(s6)).toString());
	}

}
