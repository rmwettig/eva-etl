package de.ingef.eva.configuration;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import de.ingef.eva.database.DatabaseHost;
import de.ingef.eva.query.Query;
import de.ingef.eva.query.QueryCreator;
import de.ingef.eva.query.SimpleQueryCreator;
import de.ingef.eva.utility.Alias;

public class SqlJsonInterpreterTest {

	private static DatabaseHost host;
	
	@BeforeClass
	public static void setUpOnce() {
		host = new SchemaDatabaseHostLoader().loadFromFile("src/test/resources/configuration/schema.json");
	}

	@Test
	public void testInterpret() throws JsonProcessingException, IOException {
		JsonInterpreter sqlInterpreter = new SqlJsonInterpreter(new SimpleQueryCreator(), host, null);
		
		JsonNode root = new ObjectMapper().readTree(new File("src/test/resources/configuration/sql.config.json"));
		Collection<Query> jobs = sqlInterpreter.interpret(root.path("databases"));
		
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
		String[] arr = part.split("\\|\\|';'\\|\\|");
		assertEquals(4, arr.length);
		assertEquals("';ROW_START'", arr[0].replace("select", "").trim());
		assertEquals("coalesce(trim(database1.tablename.columnname1),'')", arr[1].trim());
		assertEquals("coalesce(trim(database1.tablename.columnname2),'')", arr[2].trim());
		assertEquals("coalesce(trim(database1.tablename2.columnname3),'')", arr[3].trim());
				
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
		QueryCreator qc = new SimpleQueryCreator();
		qc.setAliasFactory(new Alias(10));
		JsonInterpreter sqlInterpreter = new SqlJsonInterpreter(qc, host, null);
		JsonNode root = new ObjectMapper().readTree(new File("src/test/resources/configuration/sql.select.latest.config.json"));
		
		Collection<Query> query = sqlInterpreter.interpret(root.path("databases"));
		assertEquals(1, query.size());
		
		Query q = query.iterator().next();
		assertNotNull(q);
		
		Collection<String> columns = q.getSelectedColumns();
		assertEquals(2, columns.size());
		
		Iterator<String> columnNames = columns.iterator();
		assertEquals("columnname1", columnNames.next());
		assertEquals("columnname2", columnNames.next());
		
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
		String[] columnFields = part.split("\\|\\|';'\\|\\|");
		assertEquals("';ROW_START'", columnFields[0].trim());
		assertEquals("coalesce(trim(a.columnname1),'')", columnFields[1].trim());
		assertEquals("coalesce(trim(a.columnname2),'')", columnFields[2].trim());
		
		//from clause
		part = sql.substring(sql.indexOf("from")+ 4, sql.indexOf("inner"));
		assertEquals("database1.tablename a", part.trim());
		
		//join
		part = sql.substring(sql.indexOf("inner join") + 10, sql.indexOf("on"));
		assertEquals("(select columnname1, max(columnname2) as columnname2 from database1.tablename group by columnname1) b", part.trim());
		
		//on
		part = sql.substring(sql.indexOf("on") + 2, sql.indexOf("where"));
		assertEquals("a.columnname1=b.columnname1", part.trim());
		
		//where
		part = sql.substring(sql.indexOf("where") + 5, sql.lastIndexOf(";"));
		assertEquals("(a.columnname2 = b.columnname2)", part.trim());
		
		assertTrue(sql.endsWith(";"));
	}
}
