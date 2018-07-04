package de.ingef.eva.etl;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.commons.codec.digest.DigestUtils;

import de.ingef.eva.configuration.Configuration;
import de.ingef.eva.configuration.append.AppendConfiguration;
import de.ingef.eva.constant.Templates;
import de.ingef.eva.data.RowElement;
import de.ingef.eva.data.SimpleRowElement;
import de.ingef.eva.data.TeradataColumnType;
import de.ingef.eva.utility.Helper;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;

@Log4j2
public class HashTransformer extends Transformer {

	private static final String OUTPUT_COLUMN_NAME = "pid_hash";
	private final Map<String, String> pid2Hash;	
	
	public HashTransformer(String db, String table, Map<String, String> hashMapping) {
		super(db, table);
		pid2Hash = hashMapping;
	}
	
	@Getter
	@RequiredArgsConstructor
	@EqualsAndHashCode(of={"pid"})
	private static class DataEntry {
		private final String pid;
		private char gender = '9';
		private LocalDate dob = LocalDate.MAX;
		private LocalDate dod = LocalDate.MAX;
		private String minKgs = "";
		private String maxKgs = "";
		private final List<String> icdCodes = new ArrayList<>(300);
		private final List<String> pznCodes = new ArrayList<>(20);
		
		public void updateGender(char value) {
			if(value < gender)
				gender = value;
		}
		
		public void updateDOB(LocalDate value) {
			dob = takeMinimumDate(dob, value);
		}
		
		public void updateDOD(LocalDate value) {
			dod = takeMinimumDate(dod, value);
		}
		
		public void addICDCode(String code) {
			icdCodes.add(code);
		}
		
		public void updateMinKgs(String value) {
			minKgs = takeMinimumKGS(minKgs, value);
		}
		
		public void updateMaxKgs(String value) {
			maxKgs = takeMinimumKGS(maxKgs, value);
		}
		
		public void addPZN(String pzn) {
			pznCodes.add(pzn);
		}
		
		private LocalDate takeMinimumDate(LocalDate left, LocalDate right) {
			return left.isBefore(right) ? left : right;
		}
		
		private String takeMinimumKGS(String left, String right) {
			return left.compareTo(right) < 0 ? left : right;
		}
	}
		
	@Override
	public Row transform(Row row) {
		if(!canProcessRow(row.getDb(), row.getTable()))
			return row;
		if(!row.getColumnName2Index().containsKey("pid")) {
			log.warn("Missing pid column in {}.{}", row.getDb(), row.getTable());
			return row;
		}
		int pidColumnIndex = row.getColumnName2Index().get("pid");
		String hash = pid2Hash.getOrDefault(row.getColumns().get(pidColumnIndex).getContent(), "");
		
		return createNewRow(row, hash);
	}

	private Row createNewRow(Row row, String hash) {
		Map<String, Integer> transformedIndices = transformIndices(row.getColumnName2Index());
		List<RowElement> transformedColumns = transformColumns(row.getColumns(), hash);
		return new Row(row.getDb(), row.getTable(), transformedColumns, transformedIndices);
	}

	private List<RowElement> transformColumns(List<RowElement> columns, String value) {
		List<RowElement> transformedColumns = new ArrayList<>(columns.size() + 1);
		transformedColumns.addAll(columns);
		transformedColumns.add(new SimpleRowElement(value, TeradataColumnType.CHARACTER));
		return transformedColumns;
	}

	private Map<String, Integer> transformIndices(Map<String, Integer> columnName2Index) {
		Map<String, Integer> transformed = new HashMap<>(columnName2Index);
		transformed.put(OUTPUT_COLUMN_NAME, columnName2Index.size());
		return transformed;
	}

	public static Transformer of(Configuration config, AppendConfiguration transformerConfig) {
		try(
				Connection connection = Helper.createConnectionWithErrorHandling(config.getUser(), config.getPassword(), config.getFullConnectionUrl());
				Statement statement = connection.createStatement();
			) {
				ExecutorService threadPool = Helper.createThreadPool(config.getThreadCount(), true);
				WaitForCallbacks.WaitForCallbacksBuilder<Map<String, DataEntry>> builder = WaitForCallbacks.<Map<String, DataEntry>>builder();
				for(String subgroup : transformerConfig.getSubgroups()) {
					System.out.println(subgroup);
					builder.promise(fetchDataForSubgroup(config, transformerConfig, subgroup, threadPool));
				}
				
				return new HashTransformer(transformerConfig.getTargetDb(), transformerConfig.getTargetTable(), createHashMapping(builder.build().getResults()));
		} catch (SQLException e) {
			log.error("Could not create hash mapping. Reason: {}", e);
			return new Transformer.NOPTransformer();
		} catch (InterruptedException | ExecutionException e) {
			log.error("Failed to assemble hash data. Reason: {}", e);
			return new Transformer.NOPTransformer();
		}		
	}

	private static CompletableFuture<Map<String, DataEntry>> fetchDataForSubgroup(Configuration config, AppendConfiguration transformerConfig, String subgroup, ExecutorService threadPool) {
		return CompletableFuture.supplyAsync(() -> {
			WaitForCallbacks.WaitForCallbacksBuilder<Map<String, DataEntry>> builder = WaitForCallbacks.<Map<String,DataEntry>>builder();
			try {
				builder.promise(fetchBaseData(config, transformerConfig, threadPool, Templates.Hashes.SELECT_MINIMUM_DATES_AND_GENDER.replaceAll("\\$subgroup", subgroup)));
				builder.promise(fetchResidenceData(config, transformerConfig, threadPool, Templates.Hashes.SELECT_MIN_MAX_KGS.replaceAll("\\$subgroup", subgroup)));
				builder.promise(fetchIcdData(config, transformerConfig, threadPool, Templates.Hashes.SELECT_ICD_CODES.replaceAll("\\$subgroup", subgroup)));
				builder.promise(fetchPznData(config, transformerConfig, threadPool, Templates.Hashes.SELECT_PZNS.replaceAll("\\$subgroup", subgroup)));
				
				List<Map<String, DataEntry >> results =	builder.build().getResults();
				mergeData(results.get(0), results.get(1), results.get(2), results.get(3));
				return results.get(0);
			} catch(SQLException | InterruptedException | ExecutionException e) {
				throw new RuntimeException(e);
			}
		},
		threadPool);
	}
	
	private static CompletableFuture<Map<String, DataEntry>> fetchBaseData(Configuration config, AppendConfiguration transformerConfig, ExecutorService threadPool, String subgroupTemplate) throws SQLException {
		return fetchFromDatabase(
				config,
				transformerConfig,
				subgroupTemplate,
				threadPool,
				HashTransformer::createBaseDataRowElement,
				HashTransformer::createBaseDataEntry,
				HashTransformer::mergeBaseDataEntries
		);
	}

	private static CompletableFuture<Map<String, DataEntry>> fetchPznData(Configuration config, AppendConfiguration transformerConfig, ExecutorService threadPool, String subgroupTemplate) throws SQLException {
		return fetchFromDatabase(
				config,
				transformerConfig,
				subgroupTemplate,
				threadPool,
				HashTransformer::createDiagnosisDataRowElement,
				HashTransformer::createPZNDataEntry,
				HashTransformer::mergePZNDataEntries
		);
	}

	private static CompletableFuture<Map<String, DataEntry>> fetchIcdData(Configuration config, AppendConfiguration transformerConfig, ExecutorService threadPool, String subgroupTemplate) throws SQLException {
		return fetchFromDatabase(
				config,
				transformerConfig,
				subgroupTemplate,
				threadPool,
				HashTransformer::createDiagnosisDataRowElement,
				HashTransformer::createDiagnosisDataEntry,
				HashTransformer::mergeDiagnosisDataEntries
		);
	}

	private static CompletableFuture<Map<String, DataEntry>> fetchResidenceData(Configuration config, AppendConfiguration transformerConfig, ExecutorService threadPool, String subgroupTemplate) throws SQLException {
		return fetchFromDatabase(
				config,
				transformerConfig,
				subgroupTemplate,
				threadPool,
				HashTransformer::createResidenceDataRowElement,
				HashTransformer::createResidenceDataEntry,
				HashTransformer::mergeResidenceDataEntries
		);
	}
	
	private static Map<String, String> createHashMapping(List<Map<String, DataEntry>> subgroupData) {
		int numberOfEntries = subgroupData.stream().collect(Collectors.summingInt(map -> map.size()));		
		Map<String, String> pid2Hash = new HashMap<>(numberOfEntries);
		subgroupData
			.stream()
			.flatMap(map -> map.entrySet().stream())
			.forEach(entry -> {
				String dataString = buildFullDataString(entry.getValue());
				pid2Hash.put(entry.getKey(), DigestUtils.sha256Hex(dataString));				
			}); 
		return pid2Hash;
	}

	private static String buildFullDataString(DataEntry data) {
		StringBuilder dataString = new StringBuilder();
		dataString.append(data.getDob());
		dataString.append("_");
		dataString.append(data.getDod());
		dataString.append("_");
		dataString.append(data.getGender());
		dataString.append("_");
		dataString.append(data.getMinKgs());
		dataString.append("_");
		dataString.append(data.getMaxKgs());
		dataString.append("_");
		dataString.append(data.getIcdCodes().stream().collect(Collectors.joining("_")));
		dataString.append("_");
		dataString.append(data.getPznCodes().stream().collect(Collectors.joining("_")));
		return dataString.toString();
	}

	/**
	 * changes the base data map inline
	 * @param pid2BaseData
	 * @param pid2ResidenceData
	 * @param pid2Icd
	 * @param pid2Pzn
	 */
	private static void mergeData(Map<String, DataEntry> pid2BaseData, Map<String, DataEntry> pid2ResidenceData, Map<String, DataEntry> pid2Icd, Map<String, DataEntry> pid2Pzn) {
		integrateDataSliceIntoBaseData(pid2BaseData, pid2ResidenceData, (baseData, residenceData) -> {
			baseData.updateMinKgs(residenceData.getMinKgs());
			baseData.updateMaxKgs(residenceData.getMaxKgs());
			return baseData;
		});
		integrateDataSliceIntoBaseData(pid2BaseData, pid2Icd, (baseData, icd) -> {
			icd.getIcdCodes().forEach(code -> baseData.addICDCode(code));
			return baseData;
		});
		integrateDataSliceIntoBaseData(pid2BaseData, pid2Pzn, (baseData, pzn) -> {
			pzn.getPznCodes().forEach(code -> baseData.addPZN(code));
			return baseData;
		});
	}
	

	private static void integrateDataSliceIntoBaseData(Map<String, DataEntry> pid2BaseData, Map<String, DataEntry> pid2Data, BiFunction<DataEntry, DataEntry, DataEntry> merger) {
		pid2Data.forEach((pid, data) -> pid2BaseData.merge(pid, data, merger));
	}
	
	/**
	 * executes a query for all year slices
	 * @param config
	 * @param appendConfig
	 * @param queryTemplate
	 * @param threadPool
	 * @param rowConverter
	 * @param columnConverter
	 * @param entryMerger
	 * @return
	 * @throws SQLException
	 */
	private static CompletableFuture<Map<String, DataEntry>> fetchFromDatabase(
			Configuration config,
			AppendConfiguration appendConfig,
			String queryTemplate,
			Executor threadPool,
			Function<ResultSet, List<RowElement>> rowConverter,
			Function<List<RowElement>, DataEntry> columnConverter,
			BiFunction<DataEntry, DataEntry, DataEntry> entryMerger) throws SQLException {
		String user = config.getUser();
		String password = config.getPassword();
		String connectionUrl = config.getFullConnectionUrl();
		String database = appendConfig.getTargetDb();
		int minYear = appendConfig.getMinYear();
		int maxYear = appendConfig.getMaxYear();
				
		return CompletableFuture.supplyAsync(() -> {
			Map<String, DataEntry> baseData = new HashMap<>(1_950_000);
			WaitForCallbacks.WaitForCallbacksBuilder<Map<String, DataEntry>> builder = WaitForCallbacks.<Map<String, DataEntry>>builder();
			for(int year = minYear; year <= maxYear; year++) {
				String query = fillQueryTemplate(queryTemplate, database, year);
				builder.promise(createFetchPromise(threadPool, rowConverter, columnConverter, user, password, connectionUrl, query));
			}
			try {
				builder
					.build()
					.getResults()
					.stream()
					.flatMap(map -> map.entrySet().stream())
					.forEach(entry -> baseData.merge(entry.getKey(), entry.getValue(), entryMerger));
				return baseData;
			} catch (ExecutionException | InterruptedException e) {
				throw new RuntimeException(e);
			}
		},
		threadPool);
	}

	/**
	 * creates a promise for db query
	 * @param threadPool
	 * @param rowConverter
	 * @param columnConverter
	 * @param user
	 * @param password
	 * @param connectionUrl
	 * @param query
	 * @return
	 */
	private static CompletableFuture<Map<String, DataEntry>> createFetchPromise(Executor threadPool,
			Function<ResultSet, List<RowElement>> rowConverter, Function<List<RowElement>, DataEntry> columnConverter,
			String user, String password, String connectionUrl, String query) {
		return CompletableFuture.supplyAsync(() -> {
			try {
				return fetchData(		
							user,
							password,
							connectionUrl,
							query,
							rowConverter,
							columnConverter
						);
			} catch (SQLException e) {
				log.error("Could not execute query '{}'", query);
			}
			return Collections.emptyMap();
		},
		threadPool
);
	}
	
	private static List<RowElement> createBaseDataRowElement(ResultSet resultSet) {
		List<RowElement> row = new ArrayList<>(4);
		try {
			row.add(new SimpleRowElement(resultSet.getString(1), TeradataColumnType.CHARACTER));
			row.add(new SimpleRowElement(resultSet.getDate(2).toString(), TeradataColumnType.CHARACTER));
			row.add(new SimpleRowElement(resultSet.getDate(3).toString(), TeradataColumnType.CHARACTER));
			row.add(new SimpleRowElement(resultSet.getString(4), TeradataColumnType.CHARACTER));
		} catch (SQLException e) {
			log.error("Could not create row elements");
		}
		return row;
	}
	
	private static DataEntry createBaseDataEntry(List<RowElement> columns) {
		String pid = columns.get(0).getContent();
		LocalDate dob = LocalDate.parse(columns.get(1).getContent());
		LocalDate dod = LocalDate.parse(columns.get(2).getContent());
		char gender = columns.get(0).getContent().charAt(0);
		DataEntry entry = new DataEntry(pid);
		entry.updateGender(gender);
		entry.updateDOB(dob);
		entry.updateDOD(dod);
		return entry;
	}
	
	private static DataEntry mergeBaseDataEntries(DataEntry previous, DataEntry next) {
		previous.updateDOB(next.getDob());
		previous.updateDOD(next.getDod());
		previous.updateGender(next.getGender());
		return previous;
	}
	
	private static List<RowElement> createResidenceDataRowElement(ResultSet resultSet) {
		List<RowElement> row = new ArrayList<>(3);
		try {
			row.add(new SimpleRowElement(resultSet.getString(1), TeradataColumnType.CHARACTER));
			row.add(new SimpleRowElement(resultSet.getString(2).toString(), TeradataColumnType.CHARACTER));
			row.add(new SimpleRowElement(resultSet.getString(3).toString(), TeradataColumnType.CHARACTER));
		} catch (SQLException e) {
			log.error("Could not create row elements");
		}
		return row;
	}
	
	private static DataEntry createResidenceDataEntry(List<RowElement> columns) {
		String pid = columns.get(0).getContent();
		String minKgs = columns.get(1).getContent();
		String maxKgs = columns.get(2).getContent();
		DataEntry entry = new DataEntry(pid);
		entry.updateMinKgs(minKgs);
		entry.updateMaxKgs(maxKgs);
		return entry;
	}
	
	private static DataEntry mergeResidenceDataEntries(DataEntry previous, DataEntry next) {
		previous.updateMinKgs(next.getMinKgs());
		previous.updateMaxKgs(next.getMaxKgs());
		return previous;
	}
		
	private static List<RowElement> createDiagnosisDataRowElement(ResultSet resultSet) {
		List<RowElement> row = new ArrayList<>(2);
		try {
			row.add(new SimpleRowElement(resultSet.getString(1), TeradataColumnType.CHARACTER));
			row.add(new SimpleRowElement(resultSet.getString(2).toString(), TeradataColumnType.CHARACTER));
		} catch (SQLException e) {
			log.error("Could not create row elements");
		}
		return row;
	}
	
	private static DataEntry createDiagnosisDataEntry(List<RowElement> columns) {
		String pid = columns.get(0).getContent();
		String icdCode = columns.get(1).getContent();
		DataEntry entry = new DataEntry(pid);
		entry.addICDCode(icdCode);
		
		return entry;
	}
	
	private static DataEntry mergeDiagnosisDataEntries(DataEntry previous, DataEntry next) {
		next.getIcdCodes().forEach(icd -> previous.addICDCode(icd));
		return previous;
	}
		
	private static DataEntry createPZNDataEntry(List<RowElement> columns) {
		String pid = columns.get(0).getContent();
		String pzn = columns.get(1).getContent();
		DataEntry entry = new DataEntry(pid);
		entry.addPZN(pzn);
		
		return entry;
	}
	
	private static DataEntry mergePZNDataEntries(DataEntry previous, DataEntry next) {
		next.getPznCodes().forEach(pzn -> previous.addPZN(pzn));
		return previous;
	}
	
	private static Map<String, DataEntry> fetchData(
			String user,
			String password,
			String connectionUrl,
			String query,
			Function<ResultSet, List<RowElement>> rowConverter,
			Function<List<RowElement>, DataEntry> columnConverter
	) throws SQLException {
		Map<String, DataEntry> dataMap = new HashMap<>(1_000_000);
		try (Connection connection = Helper.createConnection(user, password, connectionUrl)) {
			PreparedStatement statement = connection.prepareStatement(query);
			List<List<RowElement>> data = executeQuery(statement, rowConverter);
			for(List<RowElement> row : data) {
				DataEntry entry = columnConverter.apply(row);
				dataMap.put(entry.getPid(), entry);
			}
			return dataMap;
		} catch (ClassNotFoundException | SQLException e) {
			throw new RuntimeException("Could not execute query: '" + query + "'", e);
		}
	}
	
	private static List<List<RowElement>> executeQuery(PreparedStatement statement, Function<ResultSet, List<RowElement>> converter) throws SQLException {
		try (ResultSet result = statement.executeQuery()) {
			List<List<RowElement>> data = new ArrayList<>(10_000_000);
			while(result.next()) {
				data.add(converter.apply(result));
			}
			return data;
		} catch(SQLException e) {
			throw new SQLException(e);
		}
	}

	private static String fillQueryTemplate(String queryTemplate, String db, int year) {
		return queryTemplate
				.replaceAll("\\$year", Integer.toString(year))
				.replaceAll("\\$db", db);
	}
	
}
