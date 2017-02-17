package de.ingef.eva.configuration;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.util.Calendar;

import org.junit.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class ConfigurationTest {

	@Test
	public void testEndYearDefaultsToCurrentYear() throws JsonProcessingException, IOException 
	{
		ObjectMapper mapper = new ObjectMapper();
		JsonNode root = mapper.readTree(new File("src/test/resources/configuration/testNoEndYear.json"));
		Configuration config = new Configuration(root);
		
		assertEquals("127.0.0.1", config.getServer());
		assertEquals("jdbc:teradata://", config.getConnectionUrl());
		assertEquals("user", config.getUsername());
		assertEquals("pwd", config.getUserpassword());
		assertEquals("out", config.getOutDirectory());
		assertEquals("tmp", config.getTempDirectory());
		assertTrue(config.getDatabaseQueryConfiguration() != null);
		assertEquals(2010, config.getDatabaseQueryConfiguration().getStartYear());
		assertEquals(Calendar.getInstance().get(Calendar.YEAR), config.getDatabaseQueryConfiguration().getEndYear());
		assertEquals(1, config.getDatabaseQueryConfiguration().getEntries().size());
		int i = 0;
		for(DatabaseEntry dbe : config.getDatabaseQueryConfiguration().getEntries())
		{
			if(i == 0)
			{
				assertEquals("source1", dbe.getName());
				assertEquals(1, dbe.getTables().size());
				assertTrue(dbe.getTables().contains("view1"));
			}
		}
	}
	
	@Test
	public void testThreadCountValid() throws JsonProcessingException, IOException
	{
		ObjectMapper mapper = new ObjectMapper();
		String json = "{ \"threads\":\"4\"}";
		JsonNode root = mapper.readTree(json);
		Configuration config = new Configuration(root);
		assertEquals(4, config.getThreadCount());
	}
	
	@Test
	public void testThreadCountInvalidNegative() throws JsonProcessingException, IOException
	{
		ObjectMapper mapper = new ObjectMapper();
		String json = "{ \"threads\":\"-4\"}";
		JsonNode root = mapper.readTree(json);
		Configuration config = new Configuration(root);
		assertEquals(1, config.getThreadCount());
		
	}
	
	@Test
	public void testThreadCountInvalidType() throws JsonProcessingException, IOException
	{
		ObjectMapper mapper = new ObjectMapper();
		String json = "{ \"threads\":\"fd\"}";
		JsonNode root = mapper.readTree(json);
		Configuration config = new Configuration(root);
		assertEquals(1, config.getThreadCount());
	}
	
	@Test
	public void testThreadCountInvalidZero() throws JsonProcessingException, IOException
	{
		ObjectMapper mapper = new ObjectMapper();
		String json = "{ \"threads\":\"0\"}";
		JsonNode root = mapper.readTree(json);
		Configuration config = new Configuration(root);
		assertEquals(1, config.getThreadCount());
	}
}
