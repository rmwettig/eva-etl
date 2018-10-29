package de.ingef.eva.query;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Collections;

import org.junit.BeforeClass;
import org.junit.Test;

import de.ingef.eva.configuration.export.JoinType;
import de.ingef.eva.configuration.export.WhereOperator;
import de.ingef.eva.configuration.export.WhereType;
import de.ingef.eva.database.Column;
import de.ingef.eva.database.Database;
import de.ingef.eva.database.DatabaseSchema;
import de.ingef.eva.database.Table;
import de.ingef.eva.query.creation.SimpleQueryCreator;

public class SimpleQueryCreatorTest {

	private static DatabaseSchema schema;
	
	@BeforeClass
	public static void createSchemaMock() {
		Database db = new Database("DB");
		Table table1 = new Table("table");
		Column c1 = new Column("column");
		Column c2 = new Column("column2");
		Column c3 = new Column("column3");
		table1.addColumn(c1);
		table1.addColumn(c2);
		table1.addColumn(c3);
		db.addTable(table1);
		
		Table table2 = new Table("table2");
		table2.addColumn(new Column("column"));
		db.addTable(table2);
		
		DatabaseSchema dbSchema = new DatabaseSchema();
		dbSchema.addDatabase(db);
		
		schema = dbSchema;
	}
	
	@Test
	public void testQueryWithOutJoin() {
		SimpleQueryCreator creator = new SimpleQueryCreator(schema);
		/*
		  select a.column
		  from DB.table a
		  where (a.column = 1 or a.column2 = 'lol') and a.column3 like 'C%';
		 */
		
		creator.setDatabase("DB");
		creator.addTable("table");
		creator.addColumn("table", "column");
		creator.startOrGroup();
		creator.addWhere("table", "column", Collections.singletonList("1"), WhereOperator.EQUAL, WhereType.NUMERIC);
		creator.addWhere("table", "column2", Collections.singletonList("lol"), WhereOperator.EQUAL, WhereType.STRING);
		creator.endOrGroup();
		creator.startOrGroup();
		creator.addWhere("table", "column3", Collections.singletonList("C%"), WhereOperator.LIKE, WhereType.STRING);
		creator.endOrGroup();
		
		String q = creator.buildQueries().get(0).getQuery();
		
		assertTrue(q.startsWith("select"));
		String[] arr = q.substring(q.indexOf("select")+ "select".length(), q.indexOf("from")).split(",");
		assertEquals(1, arr.length);
		assertEquals("a.column", arr[0].trim());		
		
		assertTrue(q.contains("from"));
		arr = q.substring(q.indexOf("from")+ "from".length(), q.indexOf("where")).split(",");
		assertEquals(1, arr.length);
		assertEquals("DB.table a", arr[0].trim());
		
		assertTrue(q.contains("where"));
		arr = q.substring(q.indexOf("where")+ "where".length(), q.lastIndexOf(";")).split("and");
		assertEquals(2, arr.length);
		
		assertEquals("(a.column = 1 or a.column2 = 'lol')", arr[0].trim());
		assertEquals("(a.column3 like 'C%')", arr[1].trim());		
		
		assertTrue(q.contains("and"));
		assertTrue(q.contains(";"));
	}
	
	@Test
	public void testQueryWithJoin() {
		SimpleQueryCreator creator = new SimpleQueryCreator(schema);
		/*
		  select a.column, b.column
		  from DB.table a
		  inner join DB.table2 b
		  on a.primary=b.primary
		  where (a.column=1 or a.column2='lol') and (a.column3 like 'C%') and (b.column > 100);
		 */
		
		creator.setDatabase("DB");
		creator.addTable("table");
		creator.addColumn("table", "column");
		creator.startOrGroup();
		creator.addWhere("table", "column", Collections.singletonList("1"), WhereOperator.EQUAL, WhereType.NUMERIC);
		creator.addWhere("table", "column2", Collections.singletonList("lol"), WhereOperator.EQUAL, WhereType.STRING);
		creator.endOrGroup();
		creator.startOrGroup();
		creator.addWhere("table", "column3", Collections.singletonList("C%"), WhereOperator.LIKE, WhereType.STRING);
		creator.endOrGroup();
		
		creator.addColumn("table2", "column");
		creator.addJoin("table", "table2", Collections.singletonList("primary"), JoinType.INNER);
		creator.startOrGroup();
		creator.addWhere("table2", "column", Collections.singletonList("100"), WhereOperator.LARGER, WhereType.NUMERIC);
		creator.endOrGroup();
		
		String q = creator.buildQueries().get(0).getQuery();
		assertTrue("No select", q.startsWith("select"));
		
		String[] arr = q.substring(q.indexOf("select")+ "select".length(), q.indexOf("from")).split(",");
		assertEquals(2, arr.length);
		assertEquals("a.column", arr[0].trim());
		assertEquals("b.column", arr[1].trim());
		
		assertTrue("No from", q.contains("from"));
		arr = q.substring(q.indexOf("from")+ "from".length(), q.indexOf("inner")).split(",");
		assertEquals(1, arr.length);
		assertEquals("DB.table a", arr[0].trim());
		
		assertTrue("No inner join", q.contains("inner join"));
		arr = q.substring(q.indexOf("inner join")+ "inner join".length(), q.indexOf("on")).split(",");
		assertEquals(1, arr.length);
		assertEquals("DB.table2 b", arr[0].trim());
		
		assertTrue("No on", q.contains("on"));
		arr = q.substring(q.indexOf("on")+ "on".length(), q.indexOf("where")).split(",");
		assertEquals(1, arr.length);
		assertEquals("a.primary=b.primary", arr[0].trim());
				
		assertTrue("No and",q.contains("and"));
		assertTrue("No where", q.contains("where"));
		arr = q.substring(q.indexOf("where")+ "where".length(), q.lastIndexOf(";")).split("and");
		assertEquals(3, arr.length);
		assertEquals("(a.column = 1 or a.column2 = 'lol')", arr[0].trim());
		assertEquals("(a.column3 like 'C%')", arr[1].trim());
		assertEquals("(b.column > 100)", arr[2].trim());
		
		assertTrue("No terminal semicolon", q.contains(";"));
	}

	@Test
	public void constructGlobalCondition() {
		/*
		 * Expected query:
		 * select a.column
		 * from DB.table a
		 * where a.global = 1337 
		 */
		
		SimpleQueryCreator creator = new SimpleQueryCreator(schema);
		creator.setDatabase("DB");
		creator.addTable("table");
		creator.addColumn("table", "column");
		creator.addGlobalWhere("table", "global", Collections.singletonList(Integer.toString(1337)), WhereOperator.EQUAL, WhereType.NUMERIC);
		
		String q = creator.buildQueries().get(0).getQuery();
		assertTrue("No select", q.startsWith("select"));
		assertEquals("a.column", q.substring(q.indexOf("select")+ "select".length(), q.indexOf("from")).trim());
		assertTrue("No from", q.contains("from"));
		assertEquals("DB.table a", q.substring(q.indexOf("from")+ "from".length(), q.indexOf("where")).trim());
		assertEquals("(a.global = 1337)", q.substring(q.indexOf("where")+ "where".length(), q.indexOf(";")).trim());
		assertTrue("No terminal semicolon", q.contains(";"));
	}
	
	@Test
	public void removeExcludedColumns() {
		/*
		 * Expected query:
		 * select a.column
		 * from DB.table
		 */
		SimpleQueryCreator creator = new SimpleQueryCreator(schema);
		creator.setDatabase("DB");
		creator.addTable("table");
		creator.addAllKnownColumns("table", Arrays.asList("column2", "column3"));
		String q = creator.buildQueries().get(0).getQuery();
		assertNotNull(q);
		assertFalse(q.isEmpty());
		assertTrue("No select", q.startsWith("select"));
		assertEquals("a.column", q.substring(q.indexOf("select")+ "select".length(), q.indexOf("from")).trim());
		assertTrue("No from", q.contains("from"));
		assertEquals("DB.table a", q.substring(q.indexOf("from")+ "from".length(), q.indexOf(";")).trim());
		assertTrue("No terminal semicolon", q.contains(";"));
	}
	
}
