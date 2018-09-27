package de.ingef.eva.configuration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;

import org.junit.Test;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import de.ingef.eva.configuration.export.FileCondition;
import de.ingef.eva.configuration.export.WhereConfig;
import de.ingef.eva.configuration.export.WhereOperator;
import de.ingef.eva.configuration.export.WhereType;
import de.ingef.eva.configuration.export.sql.SqlNodeType;

public class PolymorphicWhereConfigTest {
		
	@Test
	public void createConditionsFromFile() throws JsonParseException, JsonMappingException, IOException, IllegalAccessException, IllegalArgumentException, InvocationTargetException, NoSuchMethodException, SecurityException {
		WhereConfig condition = new ObjectMapper().readValue(new File("src/test/resources/configuration/fileConditionWithValidPath.json"), WhereConfig.class);
		assertNotNull(condition);
		assertEquals(SqlNodeType.WHERE_FILE, condition.getType());
		assertEquals(WhereType.STRING, condition.getColumnType());
		assertEquals(WhereOperator.EQUAL, condition.getOperator());
		FileCondition fileCondition = (FileCondition) condition;
		assertNotNull(fileCondition);
		Method method = fileCondition.getClass().getDeclaredMethod("prepareConditionValues");
		method.setAccessible(true);
		@SuppressWarnings("unchecked") //API uses List<String>
		List<String> values = (List<String>) method.invoke(fileCondition);
		assertNotNull(values);
		assertFalse(values.isEmpty());
		assertEquals(3, values.size());
		assertEquals("1", values.get(0));
		assertEquals("2", values.get(1));
		assertEquals("3", values.get(2));
	}
	
	@Test
	public void emptyListIfFileCannotBeRead() throws JsonParseException, JsonMappingException, IOException, IllegalAccessException, IllegalArgumentException, InvocationTargetException, NoSuchMethodException, SecurityException {
		WhereConfig condition = new ObjectMapper().readValue(new File("src/test/resources/configuration/fileConditionWithInvalidPath.json"), WhereConfig.class);
		assertNotNull(condition);
		assertEquals(SqlNodeType.WHERE_FILE, condition.getType());
		assertEquals(WhereType.STRING, condition.getColumnType());
		assertEquals(WhereOperator.EQUAL, condition.getOperator());
		FileCondition fileCondition = (FileCondition) condition;
		assertNotNull(fileCondition);
		Method method = fileCondition.getClass().getDeclaredMethod("prepareConditionValues");
		method.setAccessible(true);
		@SuppressWarnings("unchecked") //API uses List<String>
		List<String> values = (List<String>) method.invoke(fileCondition);
		assertNotNull(values);
		assertTrue(values.isEmpty());
	}
}
