package de.ingef.eva.query;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.BeforeClass;
import org.junit.Test;

import de.ingef.eva.configuration.SchemaDatabaseHostLoader;
import de.ingef.eva.configuration.export.InlineCondition;
import de.ingef.eva.configuration.export.SourceConfig;
import de.ingef.eva.configuration.export.ViewConfig;
import de.ingef.eva.configuration.export.WhereOperator;
import de.ingef.eva.configuration.export.WhereType;
import de.ingef.eva.configuration.export.sql.ColumnNode;
import de.ingef.eva.database.DatabaseHost;
import de.ingef.eva.query.creation.QueryCreator;
import de.ingef.eva.query.creation.SimpleQueryCreator;

public class SimpleQueryCreatorIntegrationTest {

	private static DatabaseHost host;
	
	@BeforeClass
	public static void setUpOnce() {
		host = new SchemaDatabaseHostLoader().loadFromFile("src/test/resources/configuration/queryCreatorIntegrationTestSchema.json");
	}
	
	/**
	 * Expected sql query:
	 * 
	 * select a.columnname1, a.columnname2
	 * from database1.tablename a;
	 */
	@Test
	public void createsUnrestrictedQuery() {
		QueryCreator creator = new SimpleQueryCreator(host);
		ViewConfig view = new ViewConfig();
		view.setName("tablename");
		view.setColumns(Arrays.asList(new ColumnNode("columnname1"), new ColumnNode("columnname2")));
		view.setAlias("t");
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
		String[] columns = queryCode.substring(0 + "select".length(), queryCode.indexOf("from")).split(",");
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
		QueryCreator creator = new SimpleQueryCreator(host);
		ViewConfig view = new ViewConfig();
		view.setName("tablename");
		view.setColumns(Arrays.asList(new ColumnNode("columnname1"), new ColumnNode("columnname2")));
		view.setAlias("t");
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
		String[] columns = queryCode.substring(0 + "select".length(), queryCode.indexOf("from")).split(",");
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
		QueryCreator creator = new SimpleQueryCreator(host);
		ViewConfig view = new ViewConfig();
		view.setName("tablename");
		view.setColumns(Arrays.asList(new ColumnNode("columnname1"), new ColumnNode("columnname2")));
		view.setAlias("t");
		view.setWhere(Collections.singletonList(new InlineCondition("columnname1", Collections.singletonList("1337"), WhereOperator.EQUAL, WhereType.NUMERIC)));
		SourceConfig source = new SourceConfig();
		source.setDb("database1");
		source.setViews(Collections.singletonList(view));
				
		List<Query> queries = source.traverse(creator);
		assertNotNull(queries);
		assertEquals(4, queries.size());
		
		for(int i = 0; i < queries.size(); i++) {
			Query query = queries.get(i);
			assertNotNull(query);
			assertNotNull(query.getQuery());
			assertEquals("database1", query.getDbName());
			assertEquals("tablename", query.getTableName());
			
			String queryCode = query.getQuery();
			assertNotNull(queryCode);
			String[] columns = queryCode.substring(0 + "select".length(), queryCode.indexOf("from")).split(",");
			assertEquals("a.columnname1", columns[0].trim());
			assertEquals("a.columnname2", columns[1].trim());
			String fromClause = queryCode.substring(queryCode.indexOf("from") + "from".length(), queryCode.indexOf("where"));
			assertEquals("database1.tablename a", fromClause.trim());
			String whereClause = queryCode.substring(queryCode.indexOf("where") + "where".length(), queryCode.indexOf(";")).trim();
			String[] conditions = whereClause.split("and");
			assertEquals("(a.columnname1 = 1337)", conditions[0].trim());
			assertEquals("a.bezugsjahr = " + (2015 + i), conditions[1].trim());
		}
	}
	
}
