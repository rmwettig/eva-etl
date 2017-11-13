package de.ingef.eva.query;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.BeforeClass;
import org.junit.Test;

import de.ingef.eva.database.Column;
import de.ingef.eva.database.Database;
import de.ingef.eva.database.DatabaseHost;
import de.ingef.eva.database.Table;
import de.ingef.eva.database.TextColumn;
import de.ingef.eva.database.TextDatabase;
import de.ingef.eva.database.TextSchema;
import de.ingef.eva.database.TextTable;
import de.ingef.eva.query.creation.SimpleQueryCreator;

public class SimpleQueryCreatorTest {

	private static DatabaseHost schema;
	
	@BeforeClass
	public static void createSchemaMock() {
		Database db = new TextDatabase("DB");
		Table table1 = new TextTable("table");
		Column c1 = new TextColumn("column");
		Column c2 = new TextColumn("column2");
		Column c3 = new TextColumn("column3");
		table1.addColumn(c1);
		table1.addColumn(c2);
		table1.addColumn(c3);
		db.addTable(table1);
		
		Table table2 = new TextTable("table2");
		table2.addColumn(new TextColumn("column"));
		db.addTable(table2);
		
		TextSchema dbSchema = new TextSchema();
		dbSchema.addDatabase(db);
		
		schema = dbSchema;
	}
	
	@Test
	public void testQueryWithOutJoin() {
		SimpleQueryCreator creator = new SimpleQueryCreator(schema);
		/**
		 * select DB.table.column
		 * from DB.table
		 * where (DB.table.column=1 or DB.table.column2='lol') and DB.table.column3 like 'C%';
		 */
		
		creator.setDatabase("DB");
		creator.addTable("table");
		creator.addColumn("table", "column");
		creator.startOrGroup();
		creator.addWhere("table", "column", "1", "=", "NUMERIC");
		creator.addWhere("table", "column2", "lol", "=", "STRING");
		creator.endOrGroup("table");
		creator.startOrGroup();
		creator.addWhere("table", "column3", "C%", "like", "STRING");
		creator.endOrGroup("table");
		
		String q = creator.buildQuery().getQuery();
		
		assertTrue(q.startsWith("select"));
		String[] arr = q.substring(q.indexOf("select")+ "select".length(), q.indexOf("from")).split(",");
		assertEquals(1, arr.length);
		assertEquals("DB.table.column", arr[0].trim());		
		
		assertTrue(q.contains("from"));
		arr = q.substring(q.indexOf("from")+ "from".length(), q.indexOf("where")).split(",");
		assertEquals(1, arr.length);
		assertEquals("DB.table", arr[0].trim());
		
		assertTrue(q.contains("where"));
		arr = q.substring(q.indexOf("where")+ "where".length(), q.lastIndexOf(";")).split("and");
		assertEquals(2, arr.length);
		
		assertEquals("(DB.table.column = 1 or DB.table.column2 = 'lol')", arr[0].trim());
		assertEquals("(DB.table.column3 like 'C%')", arr[1].trim());		
		
		assertTrue(q.contains("and"));
		assertTrue(q.contains(";"));
	}
	
	@Test
	public void testQueryWithJoin() {
		SimpleQueryCreator creator = new SimpleQueryCreator(schema);
		/**
		 * select DB.table.column,DB.table2.column
		 * from DB.table
		 * inner join DB.table2
		 * on DB.table.primary=DB.table2.primary
		 * where (DB.table.column=1 or DB.table.column2='lol') and (DB.table.column3 like 'C%') and (DB.table2.column > 100);
		 */
		
		creator.setDatabase("DB");
		creator.addTable("table");
		creator.addColumn("table", "column");
		creator.startOrGroup();
		creator.addWhere("table", "column", "1", "=", "NUMERIC");
		creator.addWhere("table", "column2", "lol", "=", "STRING");
		creator.endOrGroup("table");
		creator.startOrGroup();
		creator.addWhere("table", "column3", "C%", "like", "STRING");
		creator.endOrGroup("table");
		
		creator.addColumn("table2", "column");
		creator.addJoin("table", "table2", "primary", "inner");
		creator.startOrGroup();
		creator.addWhere("table2", "column", "100", ">", "NUMERIC");
		creator.endOrGroup("table2");
		
		String q = creator.buildQuery().getQuery();
		assertTrue("No select", q.startsWith("select"));
		
		String[] arr = q.substring(q.indexOf("select")+ "select".length(), q.indexOf("from")).split(",");
		assertEquals(2, arr.length);
		assertEquals("DB.table.column", arr[0].trim());
		assertEquals("DB.table2.column", arr[1].trim());
		
		assertTrue("No from", q.contains("from"));
		arr = q.substring(q.indexOf("from")+ "from".length(), q.indexOf("inner")).split(",");
		assertEquals(1, arr.length);
		assertEquals("DB.table", arr[0].trim());
		
		assertTrue("No inner join", q.contains("inner join"));
		arr = q.substring(q.indexOf("inner join")+ "inner join".length(), q.indexOf("on")).split(",");
		assertEquals(1, arr.length);
		assertEquals("DB.table2", arr[0].trim());
		
		assertTrue("No on", q.contains("on"));
		arr = q.substring(q.indexOf("on")+ "on".length(), q.indexOf("where")).split(",");
		assertEquals(1, arr.length);
		assertEquals("(DB.table.primary=DB.table2.primary)", arr[0].trim());
				
		assertTrue("No and",q.contains("and"));
		assertTrue("No where", q.contains("where"));
		arr = q.substring(q.indexOf("where")+ "where".length(), q.lastIndexOf(";")).split("and");
		assertEquals(3, arr.length);
		assertEquals("(DB.table.column = 1 or DB.table.column2 = 'lol')", arr[0].trim());
		assertEquals("(DB.table.column3 like 'C%')", arr[1].trim());
		assertEquals("(DB.table2.column > 100)", arr[2].trim());
		
		assertTrue("No terminal semicolon", q.contains(";"));
	}

}
