package de.ingef.eva.data.validation;

import static org.junit.Assert.*;

import org.junit.Test;

import de.ingef.eva.data.RowElement;
import de.ingef.eva.data.SimpleRowElement;
import de.ingef.eva.data.TeradataColumnType;

public class TypeRuleTest {

	@Test
	public void modifyContentIfTypeMatches() {
		RowElement e = new SimpleRowElement("FG", 0, TeradataColumnType.CHARACTER, ").:,;mg#'+*~-_|><[]§$%&/{}\\`´!\"^°4(");
		//replace all non-numeric characters
		Rule r = new TypeRule(TeradataColumnType.CHARACTER, new ReplacePattern("[^\\d]", ""));
		RowElement cleanElement = r.validate(e); 
				
		assertEquals("4", cleanElement.getContent());
		assertEquals(e.getIndex(), cleanElement.getIndex());
		assertEquals(e.getName(), cleanElement.getName());
		assertEquals(e.getType(), cleanElement.getType());
	}
	
	@Test
	public void doNotModifyContentIfTypeDoesNotMatch() {
		RowElement e = new SimpleRowElement("Description", 0, TeradataColumnType.CHARACTER, ").:,;mg#'+*~-_|><[]§$%&/{}\\`´!\"^°4(");
		//replace all non-numeric characters
		Rule r = new TypeRule(TeradataColumnType.INTEGER, new ReplacePattern("[^\\d]", ""));
		RowElement cleanElement = r.validate(e); 
				
		assertEquals(").:,;mg#'+*~-_|><[]§$%&/{}\\`´!\"^°4(", cleanElement.getContent());
		assertEquals(e.getIndex(), cleanElement.getIndex());
		assertEquals(e.getName(), cleanElement.getName());
		assertEquals(e.getType(), cleanElement.getType());
	}

	@Test
	public void applyAnyTypeRuleNeglectingActualType() {
		RowElement e = new SimpleRowElement("ZANR", 0, TeradataColumnType.CHARACTER, "73;738");
		Rule r = new TypeRule(TeradataColumnType.ANY, new ReplacePattern(";", ""));
		
		RowElement cleanElement = r.validate(e);
		assertEquals("73738", cleanElement.getContent());
		assertEquals(e.getIndex(), cleanElement.getIndex());
		assertEquals(e.getName(), cleanElement.getName());
		assertEquals(e.getType(), cleanElement.getType());
	}
	
}
