package de.ingef.eva.configuration;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.commons.codec.digest.DigestUtils;

import de.ingef.eva.constant.Templates;
import de.ingef.eva.data.RowElement;
import de.ingef.eva.data.SimpleRowElement;
import de.ingef.eva.data.TeradataColumnType;
import de.ingef.eva.utility.CsvWriter;
import de.ingef.eva.utility.Helper;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.extern.log4j.Log4j2;

@Getter
@Setter
@NoArgsConstructor
@Log4j2
public class HashConfig {

	private Path hashFile;
	private int minYear;
	private int maxYear;
	private List<HashGroup> subgroups;
	
	@Getter @Setter
	@NoArgsConstructor
	public static class HashGroup {
		private String db;
		private List<String> subgroups;
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
	
	public void calculateHashes(Configuration config) {
		ExecutorService threadPool = Helper.createThreadPool(config.getThreadCount(), true);
		removePreviousHashFile();
		List<Map<String, DataEntry>> slices = new ArrayList<>(subgroups.size());
		for(HashGroup group : subgroups) {
			for(String subgroup : group.getSubgroups()) {
				slices.add(fetchDataForSubgroup(config, group, subgroup, threadPool));
			}
		}
		
		Map<String, String> pid2Hash = createHashMapping(slices);
		
		try {
			CsvWriter writer = CsvWriter.createUncompressedWriter(hashFile, false);
			pid2Hash
			.entrySet()
			.stream()
			.forEach(mapping -> {
				try {
					writer.addEntry(mapping.getKey());
					writer.addEntry(mapping.getValue());
					writer.writeLine();
				} catch (IOException e) {
					log.error("Could not write hashes. {}", e);
				}
			});
		} catch (FileNotFoundException e) {
			log.error("Could not create writer. {}", e);
		}
	}

	/**
	 * remove old hash file to avoid unwanted aggregation of hash mappings
	 */
	private void removePreviousHashFile() {
		try {
			Files.deleteIfExists(hashFile);
		} catch (IOException e) {
			log.error("Could not remove hash file '{}'. {}", hashFile, e);
		}
	}

	private Map<String, DataEntry> fetchDataForSubgroup(Configuration config, HashGroup groupConfig, String subgroup, ExecutorService threadPool) {
		try {		
			Map<String, DataEntry> base = fetchBaseData(config, groupConfig, threadPool, Templates.Hashes.SELECT_MINIMUM_DATES_AND_GENDER.replaceAll("\\$subgroup", subgroup));
			mergeData(
				base,
				fetchResidenceData(config, groupConfig, threadPool, Templates.Hashes.SELECT_MIN_MAX_KGS.replaceAll("\\$subgroup", subgroup)),
				fetchIcdData(config, groupConfig, threadPool, Templates.Hashes.SELECT_ICD_CODES.replaceAll("\\$subgroup", subgroup)),
				fetchPznData(config, groupConfig, threadPool, Templates.Hashes.SELECT_PZNS.replaceAll("\\$subgroup", subgroup))
			);
			return base;
		} catch(SQLException e) {
			throw new RuntimeException(e);
		}
	}
	
	private Map<String, DataEntry> fetchBaseData(Configuration config, HashGroup groupConfig, ExecutorService threadPool, String subgroupTemplate) throws SQLException {
		Map<String, DataEntry> result = fetchFromDatabase(
				config,
				groupConfig,
				subgroupTemplate,
				threadPool,
				HashConfig::createBaseDataRowElement,
				HashConfig::createBaseDataEntry,
				HashConfig::mergeBaseDataEntries
		);
		return result;
	}

	private Map<String, DataEntry> fetchPznData(Configuration config, HashGroup groupConfig, ExecutorService threadPool, String subgroupTemplate) throws SQLException {
		Map<String, DataEntry> result = fetchFromDatabase(
				config,
				groupConfig,
				subgroupTemplate,
				threadPool,
				HashConfig::createDiagnosisDataRowElement,
				HashConfig::createPZNDataEntry,
				HashConfig::mergePZNDataEntries
		);
		return result;
	}

	private Map<String, DataEntry> fetchIcdData(Configuration config, HashGroup groupConfig, ExecutorService threadPool, String subgroupTemplate) throws SQLException {
		Map<String, DataEntry> result = fetchFromDatabase(
				config,
				groupConfig,
				subgroupTemplate,
				threadPool,
				HashConfig::createDiagnosisDataRowElement,
				HashConfig::createDiagnosisDataEntry,
				HashConfig::mergeDiagnosisDataEntries
		);
		return result;
	}

	private Map<String, DataEntry> fetchResidenceData(Configuration config, HashGroup groupConfig, ExecutorService threadPool, String subgroupTemplate) throws SQLException {
		Map<String, DataEntry> result = fetchFromDatabase(
				config,
				groupConfig,
				subgroupTemplate,
				threadPool,
				HashConfig::createResidenceDataRowElement,
				HashConfig::createResidenceDataEntry,
				HashConfig::mergeResidenceDataEntries
		);
		return result;
	}
	
	private Map<String, String> createHashMapping(List<Map<String, DataEntry>> subgroupData) {		
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

	private String buildFullDataString(DataEntry data) {
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
	private void mergeData(Map<String, DataEntry> pid2BaseData, Map<String, DataEntry> pid2ResidenceData, Map<String, DataEntry> pid2Icd, Map<String, DataEntry> pid2Pzn) {
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
	

	private void integrateDataSliceIntoBaseData(Map<String, DataEntry> pid2BaseData, Map<String, DataEntry> pid2Data, BiFunction<DataEntry, DataEntry, DataEntry> merger) {
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
	private Map<String, DataEntry> fetchFromDatabase(
			Configuration config,
			HashGroup groupConfig,
			String queryTemplate,
			Executor threadPool,
			Function<ResultSet, List<RowElement>> rowConverter,
			Function<List<RowElement>, DataEntry> columnConverter,
			BiFunction<DataEntry, DataEntry, DataEntry> entryMerger) throws SQLException {
		String user = config.getUser();
		String password = config.getPassword();
		String connectionUrl = config.getFullConnectionUrl();
		String database = groupConfig.getDb();

		List<Map<String, DataEntry>> results = new ArrayList<>((maxYear - minYear) + 1);
		for(int year = minYear; year <= maxYear; year++) {
			String query = fillQueryTemplate(queryTemplate, database, year);
			results.add(createFetchPromise(threadPool, rowConverter, columnConverter, user, password, connectionUrl, query));
		}
		Map<String, DataEntry> baseData = new HashMap<>(1_950_000);
		results
		.stream()
		.flatMap(map -> map.entrySet().stream())
		.forEach(entry -> baseData.merge(entry.getKey(), entry.getValue(), entryMerger));
		return baseData;
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
	private Map<String, DataEntry> createFetchPromise(Executor threadPool,
			Function<ResultSet, List<RowElement>> rowConverter, Function<List<RowElement>, DataEntry> columnConverter,
			String user, String password, String connectionUrl, String query) {
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
		Connection connection = null;
		PreparedStatement statement = null;
		try {
			connection = Helper.createConnection(user, password, connectionUrl);
			statement = connection.prepareStatement(query);
			List<List<RowElement>> data = executeQuery(statement, rowConverter);
			if(!connection.isValid(5)) {
				statement.close();
				connection.close();
				connection = Helper.createConnection(user, password, connectionUrl);
				statement = connection.prepareStatement(query);
				log.info("Retrying hash query: {}", query);
				data = executeQuery(statement, rowConverter);
			}
			for(List<RowElement> row : data) {
				DataEntry entry = columnConverter.apply(row);
				dataMap.put(entry.getPid(), entry);
			}
			return dataMap;
		} catch (ClassNotFoundException | SQLException e) {
			throw new RuntimeException("Could not execute query: '" + query + "'", e);
		} finally {
			if(statement != null && !statement.isClosed())
				statement.close();
			if(connection != null && !connection.isClosed())
				connection.close();
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
