package de.ingef.eva.configuration;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;

import org.junit.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class ConfigurationTest {

	@Test
	public void testNoEndYearPresent() throws JsonProcessingException, IOException 
	{
		ObjectMapper mapper = new ObjectMapper();
		JsonNode root = mapper.readTree(new File("src/test/resources/testConfigNoEndYear.json"));
		Configuration config = new Configuration(root);
		
		assertEquals("127.0.0.1", config.getServer());
		assertEquals("jdbc:teradata://", config.getConnectionUrl());
		assertEquals("user", config.getUsername());
		assertEquals("pwd", config.getUserpassword());
		assertTrue(config.getDatabaseQueryConfiguration() != null);
		assertEquals(2010, config.getDatabaseQueryConfiguration().getStartYear());
		assertEquals(Calendar.getInstance().get(Calendar.YEAR), config.getDatabaseQueryConfiguration().getEndYear());
		assertTrue(config.getDatabaseQueryConfiguration().getDatabaseNames().contains("source1"));
		assertEquals(1, config.getDatabaseQueryConfiguration().getViews("source1").size());
		
		Collection<String> mustBeContained = new ArrayList<String>();
		mustBeContained.add("view1");
		assertTrue(config.getDatabaseQueryConfiguration().getViews("source1").containsAll(mustBeContained));
	}

}
