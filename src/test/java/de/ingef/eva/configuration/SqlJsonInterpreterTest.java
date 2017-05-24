package de.ingef.eva.configuration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;

import org.junit.BeforeClass;
import org.junit.Test;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import de.ingef.eva.configuration.export.ExportConfig;
import de.ingef.eva.database.Column;
import de.ingef.eva.database.DatabaseHost;
import de.ingef.eva.query.Query;
import de.ingef.eva.query.creation.QueryCreator;
import de.ingef.eva.query.creation.SimpleQueryCreator;
import de.ingef.eva.utility.Alias;

public class SqlJsonInterpreterTest {

	private static DatabaseHost host;
		
	@BeforeClass
	public static void setUpOnce() {
		host = new SchemaDatabaseHostLoader().loadFromFile("src/test/resources/configuration/schema.json");
	}

	@Test
	public void testInterpret() throws JsonProcessingException, IOException {
		SqlJsonInterpreter sqlInterpreter = new SqlJsonInterpreter(new SimpleQueryCreator(host), host);
		
		ExportConfig root = new ObjectMapper().readValue(new File("src/test/resources/configuration/sql.config.json"), ExportConfig.class);
		Collection<Query> jobs = sqlInterpreter.interpret(root);
		
		assertEquals(2, jobs.size());
		/**
		 * Expected job 1:
		 * 
		 * select database1.tablename.columnname1, database1.tablename.columnname2, database1.tablename2.columnname3
		 * from database1.tablename
		 * inner join database1.tablename2
		 * on database1.tablename.onColumn=database1.tablename2.onColumn 
		 * where 
		 * 		(database1.tablename.columnname1 = 'A')
		 * 	and (database1.tablename.commonColumn = 1337)
		 * 	and (database1.tablename2.id < 10 or database1.tablename2.id = 20 or database1.tablename2.id > 100) 
		 * ;
		 */
		Iterator<Query> jobIter = jobs.iterator();
		String job = jobIter.next().getQuery();
		
		//select clause
		String part = job.substring(0, job.indexOf("from"));
		assertTrue("No select", part.startsWith("select"));
		String[] arr = part.substring("select".length()).split(",");
		assertEquals(3, arr.length);
		assertEquals("database1.tablename.columnname1", arr[0].trim());
		assertEquals("database1.tablename.columnname2", arr[1].trim());
		assertEquals("database1.tablename2.columnname3", arr[2].trim());
				
		//from clause
		part = job.substring(job.indexOf("from"), job.indexOf("inner"));
		assertTrue("No from", part.contains("from"));
		arr = part.split(",");
		assertEquals(1, arr.length);
		assertEquals("database1.tablename", arr[0].replace("from", "").trim());

		//join clause
		part = job.substring(job.indexOf("inner"), job.indexOf("on"));
		assertTrue("No inner join", part.contains("inner join"));
		assertTrue("No inner join entry", part.contains("database1.tablename2"));
		
		//on clause
		part = job.substring(job.indexOf("on"), job.indexOf("where"));
		assertTrue("No on", part.contains("on"));
		assertTrue("No on entry", part.contains("database1.tablename.onColumn=database1.tablename2.onColumn"));
		
		//where clause
		part = job.substring(job.indexOf("where"), job.lastIndexOf(";"));
		assertTrue("No where", part.contains("where"));
		arr = part.replace("where", "").split("and");
		assertEquals(3, arr.length);
		assertEquals("(database1.tablename.columnname1 = 'A')", arr[0].trim());
		assertEquals("(database1.tablename.commonColumn = 1337)", arr[1].trim());
		assertEquals("(database1.tablename2.id < 10 or database1.tablename2.id = 20 or database1.tablename2.id > 100)", arr[2].trim());
				
		//count number of ands
		int andCount = 0;
		int start = 0;
		do{
			start = part.indexOf("and", start);
			if(start != -1)andCount++;
		}while(start++ != -1);
		assertEquals(2, andCount);
		
		assertTrue("No terminal semicolon", job.contains(";"));
		
		/**
		 * Expected job 2:
		 * select database2.tablename3.columnname4
		 * from database2.tablename3;
		 */
		job = jobIter.next().getQuery();
		//select clause
		part = job.substring(0, job.indexOf("from"));
		assertTrue("No select", part.contains("select"));
		assertTrue("No select entries", part.contains("database2.tablename3.columnname4"));
				
		//from clause
		part = job.substring(job.indexOf("from"), job.lastIndexOf(";"));
		assertTrue("No from", part.contains("from"));
		assertTrue("No from entries", part.contains("database2.tablename3"));
		
		assertTrue("No terminal semicolon", job.contains(";"));
	}
	
	@Test
	public void testLatestQuery() throws JsonProcessingException, IOException {
		QueryCreator qc = new SimpleQueryCreator(host);
		qc.setAliasFactory(new Alias(10));
		SqlJsonInterpreter sqlInterpreter = new SqlJsonInterpreter(qc, host);
		ExportConfig root = new ObjectMapper().readValue(new File("src/test/resources/configuration/sql.select.latest.config.json"), ExportConfig.class);
		
		Collection<Query> query = sqlInterpreter.interpret(root);
		assertEquals(1, query.size());
		
		Query q = query.iterator().next();
		assertNotNull(q);
		
		Collection<Column> columns = q.getSelectedColumns();
		assertEquals(2, columns.size());
		
		Iterator<Column> columnNames = columns.iterator();
		assertEquals("columnname1", columnNames.next().getName());
		assertEquals("columnname2", columnNames.next().getName());
		
		/*
		 * Expected query:
		 * select ';ROW_START'||coalesce(trim(a.columnname1),'')||';'||coalesce(trim(a.columnname2),'')
		 * from database1.tablename a
		 * inner join (select columnname1, max(columnname2) as columnname2 from database1 group by columnname1) b
		 * on a.columnname1=b.columnname1
		 * where a.columnname2=b.columnname2;
		 */
		
		String sql = q.getQuery();
		assertNotNull(sql);
		assertTrue(sql.startsWith("select"));
		
		//select clause
		String part = sql.substring(sql.indexOf("select") + 6, sql.indexOf("from"));
		String[] columnFields = part.split(",");
		assertEquals("a.columnname1", columnFields[0].trim());
		assertEquals("a.columnname2", columnFields[1].trim());
		
		//from clause
		part = sql.substring(sql.indexOf("from")+ 4, sql.indexOf("inner"));
		assertEquals("database1.tablename a", part.trim());
		
		//join
		part = sql.substring(sql.indexOf("inner join") + 10, sql.indexOf("on"));
		assertEquals("(select columnname1, max(columnname2) as columnname2 from database1.tablename group by columnname1) b", part.trim());
		
		//on
		part = sql.substring(sql.indexOf("on") + 2, sql.indexOf("where"));
		assertEquals("(a.columnname1=b.columnname1)", part.trim());
		
		//where
		part = sql.substring(sql.indexOf("where") + 5, sql.lastIndexOf(";"));
		assertEquals("(a.columnname2 = b.columnname2)", part.trim());
		
		assertTrue(sql.endsWith(";"));
	}
	
	@Test
	public void addConditionsFromFile() throws JsonParseException, JsonMappingException, IOException {
		SqlJsonInterpreter sqlInterpreter = new SqlJsonInterpreter(new SimpleQueryCreator(host), host);
		
		ExportConfig root = new ObjectMapper().readValue(new File("src/test/resources/configuration/local_where_from_file.json"), ExportConfig.class);
		Collection<Query> jobs = sqlInterpreter.interpret(root);
		
		assertEquals(1, jobs.size());
		/**
		 * Expected job 1:
		 * 
		 * select database1.tablename.columnname1, database1.tablename.columnname2
		 * from database1.tablename
		 * where 
		 * 		(database1.tablename.columnname1 = '1337' or database1.tablename.columnname1 = '1234' or database1.tablename.columnname1 = '7777')
		 * 	and (database1.tablename.commonColumn = 1337)
		 * ;
		 */
		Iterator<Query> jobIter = jobs.iterator();
		String job = jobIter.next().getQuery();
		
		//select clause
		String part = job.substring(0, job.indexOf("from"));
		assertTrue("No select", part.startsWith("select"));
		String[] arr = part.substring("select".length()).split(",");
		assertEquals(2, arr.length);
		assertEquals("database1.tablename.columnname1", arr[0].trim());
		assertEquals("database1.tablename.columnname2", arr[1].trim());
				
		//from clause
		part = job.substring(job.indexOf("from"), job.indexOf("where"));
		assertTrue("No from", part.contains("from"));
		arr = part.split(",");
		assertEquals(1, arr.length);
		assertEquals("database1.tablename", arr[0].replace("from", "").trim());
		
		//where clause
		part = job.substring(job.indexOf("where"), job.lastIndexOf(";"));
		assertTrue("No where", part.contains("where"));
		arr = part.replace("where", "").split("and");
		assertEquals(2, arr.length);
		assertEquals("(database1.tablename.columnname1 = '1337' or database1.tablename.columnname1 = '1234' or database1.tablename.columnname1 = '7777')", arr[0].trim());
		assertEquals("(database1.tablename.commonColumn = 1337)", arr[1].trim());
				
		//count number of ands
		int andCount = 0;
		int start = 0;
		do{
			start = part.indexOf("and", start);
			if(start != -1)andCount++;
		}while(start++ != -1);
		assertEquals(1, andCount);
		
		assertTrue("No terminal semicolon", job.contains(";"));
	}
	
	@Test
	public void addGlobalConditionsFromFile() throws JsonParseException, JsonMappingException, IOException {
		SqlJsonInterpreter sqlInterpreter = new SqlJsonInterpreter(new SimpleQueryCreator(host), host);
		
		ExportConfig root = new ObjectMapper().readValue(new File("src/test/resources/configuration/global_where_from_file.json"), ExportConfig.class);
		Collection<Query> jobs = sqlInterpreter.interpret(root);
		
		assertEquals(1, jobs.size());
		/**
		 * Expected job 1:
		 * 
		 * select database1.tablename.columnname1, database1.tablename.columnname2
		 * from database1.tablename
		 * where 
		 * 		(database1.tablename.columnname1 = '1337' or database1.tablename.columnname1 = '1234' or database1.tablename.columnname1 = '7777')
		 * 	and (database1.tablename.commonColumn = 1337 or database1.tablename.commonColumn = 1234 or database1.tablename.commonColumn = 7777)
		 * ;
		 */
		Iterator<Query> jobIter = jobs.iterator();
		String job = jobIter.next().getQuery();
		
		//select clause
		String part = job.substring(0, job.indexOf("from"));
		assertTrue("No select", part.startsWith("select"));
		String[] arr = part.substring("select".length()).split(",");
		assertEquals(2, arr.length);
		assertEquals("database1.tablename.columnname1", arr[0].trim());
		assertEquals("database1.tablename.columnname2", arr[1].trim());
				
		//from clause
		part = job.substring(job.indexOf("from"), job.indexOf("where"));
		assertTrue("No from", part.contains("from"));
		arr = part.split(",");
		assertEquals(1, arr.length);
		assertEquals("database1.tablename", arr[0].replace("from", "").trim());
		
		//where clause
		part = job.substring(job.indexOf("where"), job.lastIndexOf(";"));
		assertTrue("No where", part.contains("where"));
		arr = part.replace("where", "").split("and");
		assertEquals(2, arr.length);
		assertEquals("(database1.tablename.columnname1 = '1337' or database1.tablename.columnname1 = '1234' or database1.tablename.columnname1 = '7777')", arr[0].trim());
		assertEquals("(database1.tablename.commonColumn = 1337 or database1.tablename.commonColumn = 1234 or database1.tablename.commonColumn = 7777)", arr[1].trim());
				
		//count number of ands
		int andCount = 0;
		int start = 0;
		do{
			start = part.indexOf("and", start);
			if(start != -1)andCount++;
		}while(start++ != -1);
		assertEquals(1, andCount);
		
		assertTrue("No terminal semicolon", job.contains(";"));
	}
}
