package de.ingef.eva.query;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import de.ingef.eva.configuration.export.sql.DynamicYearSlice;
import org.junit.Test;

import de.ingef.eva.configuration.export.InlineCondition;
import de.ingef.eva.configuration.export.SourceConfig;
import de.ingef.eva.configuration.export.ViewConfig;
import de.ingef.eva.configuration.export.WhereOperator;
import de.ingef.eva.configuration.export.WhereType;
import de.ingef.eva.configuration.export.sql.ColumnNode;
import de.ingef.eva.data.TeradataColumnType;
import de.ingef.eva.database.Column;
import de.ingef.eva.database.Database;
import de.ingef.eva.database.DatabaseSchema;
import de.ingef.eva.database.Table;
import de.ingef.eva.query.creation.QueryCreator;
import de.ingef.eva.query.creation.SimpleQueryCreator;

public class SimpleQueryCreatorIntegrationTest {
	
	/**
	 * Expected sql query:
	 * 
	 * select a.columnname1, a.columnname2
	 * from database1.tablename a;
	 */
	@Test
	public void createsUnrestrictedQuery() {
		DatabaseSchema host = new DatabaseSchema();
		Table t1 = new Table("tablename");
		t1.addColumn(new Column("columnname1", TeradataColumnType.CHARACTER));
		t1.addColumn(new Column("columnname2", TeradataColumnType.INTEGER));
		t1.addColumn(new Column("bezugsjahr", TeradataColumnType.INTEGER));
		Database db1 = new Database("database1");
		db1.addTable(t1);
		Table t2 = new Table("tablename2");
		t2.addColumn(new Column("columnname3", TeradataColumnType.CHARACTER));
		Table t3 = new Table("tablename3");
		t3.addColumn(new Column("columnname4", TeradataColumnType.INTEGER));
		Database db2 = new Database("database2");
		db2.addTable(t3);
		host.addDatabase(db1);
		host.addDatabase(db2);
		
		QueryCreator creator = new SimpleQueryCreator(host);
		ViewConfig view = new ViewConfig();
		view.setName("tablename");
		view.setColumns(Arrays.asList(new ColumnNode("columnname1"), new ColumnNode("columnname2")));
		SourceConfig source = new SourceConfig();
		source.setDb("database1");
		source.setViews(Collections.singletonList(view));
		//null suppresses a sliced query
		source.setYearSlice(null);
		
		Query query = source.traverse(creator).get(0);
		assertNotNull(query);
		assertNotNull(query.getQuery());
		assertEquals("database1", query.getDbName());
		assertEquals("tablename", query.getTableName());
		
		String queryCode = query.getQuery();
		assertNotNull(queryCode);
		String[] columns = queryCode.substring("select".length(), queryCode.indexOf("from")).split(",");
		assertEquals("a.columnname1", columns[0].trim());
		assertEquals("a.columnname2", columns[1].trim());
		String fromClause = queryCode.substring(queryCode.indexOf("from") + "from".length(), queryCode.indexOf(";"));
		assertEquals("database1.tablename a", fromClause.trim());
	}
	
	/**
	 * Expected sql query:
	 * 
	 * select a.columnname1, a.columnname2
	 * from database1.tablename a
	 * where (a.columnname1 = 1337)
	 */
	@Test
	public void createQueryWithGlobalCondition() {
		DatabaseSchema host = new DatabaseSchema();
		Table t1 = new Table("tablename");
		t1.addColumn(new Column("columnname1", TeradataColumnType.CHARACTER));
		t1.addColumn(new Column("columnname2", TeradataColumnType.INTEGER));
		t1.addColumn(new Column("bezugsjahr", TeradataColumnType.INTEGER));
		Database db1 = new Database("database1");
		db1.addTable(t1);
		Table t2 = new Table("tablename2");
		t2.addColumn(new Column("columnname3", TeradataColumnType.CHARACTER));
		Table t3 = new Table("tablename3");
		t3.addColumn(new Column("columnname4", TeradataColumnType.INTEGER));
		Database db2 = new Database("database2");
		db2.addTable(t3);
		host.addDatabase(db1);
		host.addDatabase(db2);
		
		QueryCreator creator = new SimpleQueryCreator(host);
		ViewConfig view = new ViewConfig();
		view.setName("tablename");
		view.setColumns(Arrays.asList(new ColumnNode("columnname1"), new ColumnNode("columnname2")));
		view.setWhere(Collections.singletonList(new InlineCondition("columnname1", Collections.singletonList("1337"), WhereOperator.EQUAL, WhereType.NUMERIC)));
		SourceConfig source = new SourceConfig();
		source.setDb("database1");
		source.setViews(Collections.singletonList(view));
		//null suppresses a sliced query
		source.setYearSlice(null);
				
		Query query = source.traverse(creator).get(0);
		assertNotNull(query);
		assertNotNull(query.getQuery());
		assertEquals("database1", query.getDbName());
		assertEquals("tablename", query.getTableName());
		
		String queryCode = query.getQuery();
		assertNotNull(queryCode);
		String[] columns = queryCode.substring("select".length(), queryCode.indexOf("from")).split(",");
		assertEquals("a.columnname1", columns[0].trim());
		assertEquals("a.columnname2", columns[1].trim());
		String fromClause = queryCode.substring(queryCode.indexOf("from") + "from".length(), queryCode.indexOf("where"));
		assertEquals("database1.tablename a", fromClause.trim());
		String whereClause = queryCode.substring(queryCode.indexOf("where") + "where".length(), queryCode.indexOf(";")).trim();
		assertEquals("(a.columnname1 = 1337)", whereClause);
	}

	/**
	 * Expected queries:
	 * 
	 * select a.columnname1, a.columnname2
	 * from database1.tablename a
	 * where (a.columnname1 = 1337) and a.bezugsjahr = 2015
	 * 
	 * select a.columnname1, a.columnname2
	 * from database1.tablename a
	 * where (a.columnname1 = 1337) and a.bezugsjahr = 2016
	 * 
	 * select a.columnname1, a.columnname2
	 * from database1.tablename a
	 * where (a.columnname1 = 1337) and a.bezugsjahr = 2017
	 * 
	 * select a.columnname1, a.columnname2
	 * from database1.tablename a
	 * where (a.columnname1 = 1337) and a.bezugsjahr = 2018
	 */
	@Test
	public void createDefaultSlicedQueriesWithGlobalCondition() {
		DatabaseSchema host = new DatabaseSchema();
		Table t1 = new Table("tablename");
		t1.addColumn(new Column("columnname1", TeradataColumnType.CHARACTER));
		t1.addColumn(new Column("columnname2", TeradataColumnType.INTEGER));
		t1.addColumn(new Column("bezugsjahr", TeradataColumnType.INTEGER));
		Database db1 = new Database("database1");
		db1.addTable(t1);
		Table t2 = new Table("tablename2");
		t2.addColumn(new Column("columnname3", TeradataColumnType.CHARACTER));
		Table t3 = new Table("tablename3");
		t3.addColumn(new Column("columnname4", TeradataColumnType.INTEGER));
		Database db2 = new Database("database2");
		db2.addTable(t3);
		host.addDatabase(db1);
		host.addDatabase(db2);
		
		QueryCreator creator = new SimpleQueryCreator(host);
		ViewConfig view = new ViewConfig();
		view.setName("tablename");
		view.setColumns(Arrays.asList(new ColumnNode("columnname1"), new ColumnNode("columnname2")));
		view.setWhere(Collections.singletonList(new InlineCondition("columnname1", Collections.singletonList("1337"), WhereOperator.EQUAL, WhereType.NUMERIC)));
		SourceConfig source = new SourceConfig();
		source.setDb("database1");
		source.setViews(Collections.singletonList(view));
				
		List<Query> queries = source.traverse(creator);
		assertNotNull(queries);
		assertEquals(4, queries.size());
		List<Integer> years = new DynamicYearSlice("", 3).calculateYearRange();
		for(int i = 0; i < queries.size(); i++) {
			Query query = queries.get(i);
			assertNotNull(query);
			assertNotNull(query.getQuery());
			assertEquals("database1", query.getDbName());
			assertEquals("tablename", query.getTableName());
			
			String queryCode = query.getQuery();
			assertNotNull(queryCode);
			String[] columns = queryCode.substring("select".length(), queryCode.indexOf("from")).split(",");
			assertEquals("a.columnname1", columns[0].trim());
			assertEquals("a.columnname2", columns[1].trim());
			String fromClause = queryCode.substring(queryCode.indexOf("from") + "from".length(), queryCode.indexOf("where"));
			assertEquals("database1.tablename a", fromClause.trim());
			String whereClause = queryCode.substring(queryCode.indexOf("where") + "where".length(), queryCode.indexOf(";")).trim();
			String[] conditions = whereClause.split("and");
			assertEquals("(a.columnname1 = 1337)", conditions[0].trim());
			assertEquals("a.bezugsjahr = " + years.get(i), conditions[1].trim());
		}
	}
	
}
