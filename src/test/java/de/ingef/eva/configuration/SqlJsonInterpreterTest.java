package de.ingef.eva.configuration;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;

import org.junit.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import de.ingef.eva.database.DatabaseHost;
import de.ingef.eva.query.Query;
import de.ingef.eva.query.SimpleQueryCreator;

public class SqlJsonInterpreterTest {

	@Test
	public void testInterpret() throws JsonProcessingException, IOException {
		DatabaseHost host = new SchemaDatabaseHostLoader().loadFromFile("src/test/resources/configuration/schema.json");
		
		JsonInterpreter sqlInterpreter = new SqlJsonInterpreter(new SimpleQueryCreator(), host, null);
		JsonNode root = new ObjectMapper().readTree(new File("src/test/resources/configuration/sql.config.json"));
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
		assertTrue("No select", part.contains("select"));
		assertTrue("No select entries", part.contains("database1.tablename.columnname1,database1.tablename.columnname2,database1.tablename2.columnname3"));
		
		//from clause
		part = job.substring(job.indexOf("from"), job.indexOf("inner"));
		assertTrue("No from", part.contains("from"));
		assertTrue("No from entries", part.contains("database1.tablename"));

		//join clause
		part = job.substring(job.indexOf("inner"), job.indexOf("on"));
		assertTrue("No inner join", part.contains("inner join"));
		assertTrue("No inner join entry", part.contains("database1.tablename2"));
		
		//on clause
		part = job.substring(job.indexOf("on"), job.indexOf("where"));
		assertTrue("No on", part.contains("on"));
		assertTrue("No on entry", part.contains("database1.tablename.onColumn=database1.tablename2.onColumn"));
		
		//where clause
		part = job.substring(job.indexOf("where"), job.indexOf(";"));
		assertTrue("No where", part.contains("where"));
		assertTrue("No where columnname1A", part.contains("(database1.tablename.columnname1 = 'A')"));
		assertTrue("No where commonColumn", part.contains("(database1.tablename.commonColumn = 1337)"));
		assertTrue("No where id OrGroup", part.contains("(database1.tablename2.id < 10 or database1.tablename2.id = 20 or database1.tablename2.id > 100)"));
		
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
		part = job.substring(job.indexOf("from"), job.indexOf(";"));
		assertTrue("No from", part.contains("from"));
		assertTrue("No from entries", part.contains("database2.tablename3"));
		
		assertTrue("No terminal semicolon", job.contains(";"));
	}

}
