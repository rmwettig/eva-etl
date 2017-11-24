package de.ingef.eva.measures.cci;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

import de.ingef.eva.configuration.Configuration;
import de.ingef.eva.configuration.cci.CCISource;
import de.ingef.eva.constant.Templates;
import de.ingef.eva.utility.CsvWriter;
import de.ingef.eva.utility.Helper;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;

/**
 * Runs the calculation of charlson scores for both ADB and FDB for tables 'arzt_diagnose' and 'kh_diagnose'.
 * @author Martin.Wettig
 *
 */
@Log4j2
public final class CalculateCharlsonScores {
	
	@Getter
	@RequiredArgsConstructor
	private static class QuarterInterval {
		private final int startYear;
		private final int startQuarter;
		private final int endYear;
		private final int endQuarter;
	}
	
	@Getter
	@RequiredArgsConstructor
	private static class CCIEntry {
		private final String icdCode;
		private final String diseaseClass;
		private final int score;
	}
	
	@Getter
	@RequiredArgsConstructor
	private static class QuarterQuery {
		private final String db;
		private final String dataset;
		private final String table;
		private final String Query;
		private final int startQuarter;
		private final int startYear;
	}
	
	private static final Map<Integer,String> quarter2BeginDate = new HashMap<>();
	private static final Map<Integer,String> quarter2EndDate = new HashMap<>();
		
	public static void calculate(Configuration config) {
		initializeQuarterLimits();
		Connection conn = createICDPatterns(config);
		Collection<QuarterInterval> quarters = generateQuarters(config.getCci().getStartYear(), config.getCci().getEndYear());
		Collection<QuarterQuery> quarterQueries = createQuarterwiseSlicedQueries(config, quarters);
		
		//do not create superfluous threads
		int threadCount = Math.min(quarterQueries.size(), config.getThreadCount());
		ExecutorService threadPool = Helper.createThreadPool(threadCount, true);
		CountDownLatch cdl = new CountDownLatch(quarterQueries.size());
		executeQueries(config, quarterQueries, threadPool, cdl);
		try {
			cdl.await();
		} catch (InterruptedException e) {
			log.error("CCI calculation was interrupted. ", e);
		} finally {
			removeICDPatterns(conn, config);
		}
	}

	private static void initializeQuarterLimits() {
		quarter2BeginDate.put(1, "0101");
		quarter2BeginDate.put(2, "0401");
		quarter2BeginDate.put(3, "0701");
		quarter2BeginDate.put(4, "1001");
		
		quarter2EndDate.put(1, "0331");
		quarter2EndDate.put(2, "0630");
		quarter2EndDate.put(3, "0930");
		quarter2EndDate.put(4, "1231");
	}

	private static Collection<QuarterQuery> createQuarterwiseSlicedQueries(Configuration config, Collection<QuarterInterval> quarters) {
		Collection<QuarterQuery> quarterQueries = new ArrayList<>(200);
		for(CCISource source : config.getCci().getSources()) {
			String tempDb = config.getCci().getTempDb();
			if(source.getDb().toLowerCase().contains("adb"))
				quarterQueries.addAll(generateADBQueries(quarters, tempDb, source));
			else if(source.getDb().toLowerCase().contains("fdb"))
				quarterQueries.addAll(generateFDBQueries(quarters, tempDb, source));
		}
		return quarterQueries;
	}

	private static void executeQueries(Configuration config, Collection<QuarterQuery> queries, ExecutorService threadPool, CountDownLatch cdl) {
		System.out.println("Calculating scores");
		queries
			.stream()
			.forEach(query -> {
				CompletableFuture.supplyAsync(
					() -> {
						try {
							String dbName = query.getDb().split("_")[1];
							Path filePath = Paths.get(config.getCacheDirectory(), dbName, query.getDataset());
							if(Files.notExists(filePath)) {
								Files.createDirectories(filePath);
							}
							Path file = filePath.resolve("cci_" + query.getTable() + "." + query.getStartYear() + query.getStartQuarter() +".csv");
							Class.forName("com.teradata.jdbc.TeraDriver");
							try (
								Connection conn = DriverManager.getConnection(config.getFullConnectionUrl(), config.getUser(), config.getPassword());
								PreparedStatement ps = conn.prepareStatement(query.getQuery());
								ResultSet result = ps.executeQuery();
							) {
								CsvWriter writer = new CsvWriter(file.toFile());
								writer.open();
								Map<String,List<QuarterEntry>> pid2Data = mapPidOntoData(result);
								processSlidingQuarterWindow(query, writer, pid2Data);
								writer.close();
							} catch (SQLException e) {
								throw new RuntimeException("Could not execute query: '"+ query.getQuery() +"'", e);
							}
						} catch (ClassNotFoundException e) {
							throw new RuntimeException("Could not load Teradata JDBC driver", e);
						} catch (IOException e1) {
							throw new RuntimeException("Could not create output path", e1);
						} finally {
							cdl.countDown();
						}
						return null;
					},
					threadPool)
				.exceptionally(e -> {
					log.error("Error during CCI calculation: {} ", e.getMessage(), e);
					return null;
				});
			});
	}

	private static void processSlidingQuarterWindow(QuarterQuery query, CsvWriter writer, Map<String, List<QuarterEntry>> pid2Data) throws IOException {
		CCICalculator cciCalculator = new CCICalculator();
		writer.addEntry("pid");
		writer.addEntry("startQuarter");
		writer.addEntry("endQuarter");
		writer.addEntry("weight");
		writer.addEntry("h2ik");
		writer.writeLine();
		//iterate over data and calculate per quarter score
		pid2Data
			.forEach((pid, data) -> {
				QuarterScoreResult qsr = cciCalculator.calculateSlidingWindow(data, query.getStartYear(), query.getStartYear());
				writer.addEntry(pid);
				writer.addEntry(qsr.getStart().getYear() + quarter2BeginDate.get(qsr.getStart().getQuarter()));
				writer.addEntry(qsr.getEnd().getYear() + quarter2EndDate.get(qsr.getEnd().getQuarter()));
				writer.addEntry(Integer.toString(qsr.getWeight()));
				writer.addEntry(data.get(0).getH2ik());
				try {
					writer.writeLine();
				} catch (IOException e1) {
					throw new RuntimeException("Could not write line to file. ", e1);
				}
			});
	}
	
	/**
	 * sets up the temporary icd table
	 * @param config
	 * @return connection used to create the temp table
	 */
	private static Connection createICDPatterns(Configuration config) {
		Connection conn = null;
		try {
			Class.forName("com.teradata.jdbc.TeraDriver");
			Statement query = null;
			try {
				conn = DriverManager.getConnection(config.getFullConnectionUrl(), config.getUser(), config.getPassword());
				query = conn.createStatement();
				createICDPatternTable(config, query);
				List<CCIEntry> values = readCCIEntriesFromFile(config.getCci().getCciFile());
				insertICDPatterns(config, query, values);
			} catch (SQLException e) {
				log.error("Could not open connection or creating query.\n\tReason: ", e);
			} catch (IOException e1) {
				log.error("Could not write charlsonscores.csv.\n\tReason: ", e1);
			} finally {
				try {
					if(query != null && !query.isClosed()) {
						query.close();
					}
				} catch(SQLException e) {
					log.error("Could not close statement. ", e);
				}
			}
			return conn; 
		} catch (ClassNotFoundException e) {
			log.error("Did not found Teradata JDBC driver.");
		}
		return conn;		
	}
	
	private static List<CCIEntry> readCCIEntriesFromFile(Path path) throws IOException {
		BufferedReader reader = Files.newBufferedReader(path, StandardCharsets.UTF_8);
		return reader
			.lines()
			.skip(1)
			.filter(line -> line != null && !line.isEmpty())
			.map(line -> line.split(";"))
			.filter(columns -> columns != null && columns.length > 0)
			.map(columns -> new CCIEntry(columns[0], columns[1], Integer.parseInt(columns[2])))
			.collect(Collectors.toList());
	}

	private static Collection<QuarterInterval> generateQuarters(int startYear, int endYear) {
		int currentStartYear = startYear;
		int currentStartQuarter = 1;
		int quarterCount = (endYear - startYear + 1) * 4;
		List<QuarterInterval> quarters = new ArrayList<>(quarterCount);
		while(currentStartYear <= endYear) {
			if(currentStartQuarter > 1 && currentStartQuarter <= 4)
				quarters.add(new QuarterInterval(currentStartYear, currentStartQuarter, currentStartYear, currentStartQuarter - 1));
			else
				quarters.add(new QuarterInterval(currentStartYear, currentStartQuarter, currentStartYear + 1, 4));
			currentStartQuarter++;
			if(currentStartQuarter > 4) {
				currentStartQuarter = 1;
				currentStartYear++;
			}
		}
		return quarters;
	}
	
	private static Collection<QuarterQuery> generateADBQueries(Collection<QuarterInterval> quarters, String tempDb, CCISource source) {
		int queryCount = quarters.size() * 2;
		Collection<QuarterQuery> queries = new ArrayList<>(queryCount);
		String iks = source.getIks()
				.stream()
				.map(ik -> "'"+ik+"'")
				.collect(Collectors.joining(","));
		for(QuarterInterval interval : quarters) {
			queries.add(createADBQuery(tempDb, source, iks, interval, "arzt_diagnose", Templates.CCI.SELECT_ADB_ARZT_DIAG_WEIGHTS));
			queries.add(createADBQuery(tempDb, source, iks, interval, "kh_diagnose", Templates.CCI.SELECT_ADB_KH_DIAG_WEIGHTS));
		}
		
		return queries;
	}

	private static QuarterQuery createADBQuery(String user, CCISource source, String iks, QuarterInterval interval, String table, String queryTemplate) {
		return new QuarterQuery(
				source.getDb(),
				source.getDataset(),
				table,
				String.format(queryTemplate, user, "("+iks+")", interval.getStartYear(), interval.getStartQuarter(), interval.getEndYear(), interval.getEndQuarter()),
				interval.getStartQuarter(),
				interval.getStartYear()
			);
	}
	
	private static Collection<QuarterQuery> generateFDBQueries(Collection<QuarterInterval> quarters, String tempDb, CCISource source) {
		int queryCount = quarters.size() * 2;
		Collection<QuarterQuery> queries = new ArrayList<>(queryCount);
		for(QuarterInterval interval : quarters) {
			queries.add(createFDBQuery(tempDb, source, interval, "arzt_diagnose", Templates.CCI.SELECT_FDB_ARZT_DIAG_WEIGHTS));
			queries.add(createFDBQuery(tempDb, source, interval, "kh_diagnose", Templates.CCI.SELECT_FDB_KH_DIAG_WEIGHTS));
		}
		
		return queries;
	}

	private static QuarterQuery createFDBQuery(String user, CCISource source, QuarterInterval interval, String table, String queryTemplate) {
		return new QuarterQuery(
				source.getDb(),
				source.getDataset(),
				table,
				String.format(
						queryTemplate,
						user,
						source.getFlag(),
						interval.getStartYear(),
						interval.getStartQuarter(),
						interval.getEndYear(),
						interval.getEndQuarter()),
				interval.getStartQuarter(), interval.getStartYear());
	}
	
	/**
	 * calculates the charlson scores for a 4 quarter time span. Expected order of columns is:
	 * 		pid, quarter, year, icd, disease class, weight
	 * @param fourQuarterWindow
	 * @return
	 * @throws SQLException
	 */
	private static Map<String,List<QuarterEntry>> mapPidOntoData(ResultSet fourQuarterWindow) throws SQLException {
		Map<String,List<QuarterEntry>> pid2Data = new HashMap<>();
		while(fourQuarterWindow.next()) {

			String pid = fourQuarterWindow.getString(1);
			int quarter = fourQuarterWindow.getInt(2);
			int year = fourQuarterWindow.getInt(3);
			String icdCode = fourQuarterWindow.getString(4);
			String diseaseClass = fourQuarterWindow.getString(5);
			int weight = fourQuarterWindow.getInt(6);
			String h2ik = fourQuarterWindow.getString(7);
			if(pid2Data.containsKey(pid)) {
				pid2Data.get(pid).add(new QuarterEntry(quarter, year, icdCode, diseaseClass, weight, h2ik));
			} else {
				List<QuarterEntry> data = new ArrayList<>(100_000);
				data.add(new QuarterEntry(quarter, year, icdCode, diseaseClass, weight, h2ik));
				pid2Data.put(pid, data);
			}
		}
		return pid2Data;
	}
	
	private static void createICDPatternTable(Configuration config, Statement statement) throws SQLException {
		dropExistingICDPatterns(statement, config);
		System.out.println("Creating icd pattern table");
		statement.execute(String.format(Templates.CCI.CREATE_CCI_ICD_PATTERN_TABLE, config.getCci().getTempDb()));
	}
	
	/**
	 * 
	 * @param config
	 * @param statement
	 * @param values cci data
	 * @throws SQLException
	 */
	private static void insertICDPatterns(Configuration config, Statement statement, List<CCIEntry> values) throws SQLException {
		System.out.println("Inserting patterns");
		String insertQueries = values
				.stream()
				.map(e -> String.format(Templates.CCI.INSERT_CCI_ICD_PATTERNS, config.getCci().getTempDb(), e.getIcdCode() + '%', e.getDiseaseClass(), e.getScore()))
				.collect(Collectors.joining(""));
		statement.execute(insertQueries);
	}
	
	private static void removeICDPatterns(Connection conn, Configuration config) {
		if(conn == null) return;
		String sql = String.format(Templates.CCI.DELETE_CCI_ICD_PATTERNS, config.getCci().getTempDb());
		try(Statement query = conn.createStatement()) {
			query.executeQuery(sql);
			System.out.println("Successfully deleted icd patterns table");
		} catch (SQLException e) {
			log.error("Could not execute query '{}'. ",  sql, e);
		} finally {
			try {
				if(!conn.isClosed()) {
					conn.close();
				}
			} catch (SQLException e) {
				log.error("Could not close ICD pattern connection.", e);
			}
		}
	}
	
	private static void dropExistingICDPatterns(Statement statement, Configuration config) throws SQLException {
		ResultSet result = statement.executeQuery("select * from dbc.tables where tablename ='" + Templates.CCI.CCI_PATTERN_TABLE + "';");
		result.next();
		if(result.isLast()) {
			System.out.println("Deleting existing pattern table");
			statement.executeQuery(String.format(Templates.CCI.DELETE_CCI_ICD_PATTERNS, config.getCci().getTempDb()));
		}
	}
}
