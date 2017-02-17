package de.ingef.eva.processor;

import static org.junit.Assert.*;

import org.junit.Test;

public class RemoveBoundaryWhitespacesTest {

	@Test
	public void testProcess()
	{
		RemoveBoundaryWhitespaces p = new RemoveBoundaryWhitespaces();
		//has leading and trailing whitespaces
		StringBuilder s3 = new StringBuilder("  key2");
		StringBuilder s4 = new StringBuilder("  value2  ");	
		//whitespaces removed
		assertEquals("key2", p.process(s3).toString());
		assertEquals("value2", p.process(s4).toString());
	}

}
