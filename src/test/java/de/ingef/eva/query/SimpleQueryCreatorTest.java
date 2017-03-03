package de.ingef.eva.query;

import static org.junit.Assert.*;

import org.junit.Test;

public class SimpleQueryCreatorTest {

	@Test
	public void testQueryWithOutJoin() {
		SimpleQueryCreator creator = new SimpleQueryCreator();
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
		assertTrue(arr[0].trim().equals("DB.table.column"));
		
		
		assertTrue(q.contains("from"));
		arr = q.substring(q.indexOf("from")+ "from".length(), q.indexOf("where")).split(",");
		assertEquals(1, arr.length);
		assertEquals(1, arr.length);
		assertTrue(arr[0].trim().equals("DB.table"));
		
		assertTrue(q.contains("where"));
		arr = q.substring(q.indexOf("where")+ "where".length(), q.indexOf(";")).split("and");
		assertEquals(2, arr.length);
		
		assertTrue(arr[0].trim().equals("(DB.table.column = 1 or DB.table.column2 = 'lol')"));
		assertTrue(arr[1].trim().equals("(DB.table.column3 like 'C%')"));		
		assertTrue(q.contains("and"));
		
		assertTrue(q.contains(";"));
	}
	
	@Test
	public void testQueryWithJoin() {
		SimpleQueryCreator creator = new SimpleQueryCreator();
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
		assertEquals("DB.table.primary=DB.table2.primary", arr[0].trim());
				
		assertTrue("No and",q.contains("and"));
		assertTrue("No where", q.contains("where"));
		arr = q.substring(q.indexOf("where")+ "where".length(), q.indexOf(";")).split("and");
		assertEquals(3, arr.length);
		assertEquals("(DB.table.column = 1 or DB.table.column2 = 'lol')", arr[0].trim());
		assertEquals("(DB.table.column3 like 'C%')", arr[1].trim());
		assertEquals("(DB.table2.column > 100)", arr[2].trim());
		
		assertTrue("No terminal semicolon", q.contains(";"));
	}

}
