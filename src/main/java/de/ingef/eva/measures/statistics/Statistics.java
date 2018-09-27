package de.ingef.eva.measures.statistics;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import de.ingef.eva.configuration.Configuration;
import de.ingef.eva.configuration.statistics.StatisticDatasetConfig;
import de.ingef.eva.constant.OutputDirectory.DirectoryType;
import de.ingef.eva.constant.Templates;
import de.ingef.eva.measures.cci.Quarter;
import de.ingef.eva.utility.Helper;
import de.ingef.eva.utility.io.IOManager;
import de.ingef.eva.utility.QuarterCount;
import de.ingef.eva.utility.SystemCall;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import lombok.extern.log4j.Log4j2;

@RequiredArgsConstructor
@Log4j2
public class Statistics {
	
	private static interface SqlResultCollector {
		void collect(ResultSet resultSet) throws SQLException;
	}
	
	@Getter
	private static class OverviewStatisticsCollector implements SqlResultCollector {
		private final List<QuarterCount> data = new ArrayList<>(100);
		@Override
		public void collect(ResultSet resultSet) throws SQLException {
			int year = resultSet.getInt(1);
			int quarter = resultSet.getInt(2);
			int count = resultSet.getInt(3);
			data.add(new QuarterCount(new Quarter(year, quarter), count));
		}
	}
	
	@Getter
	private static class DetailsStatisticsCollector implements SqlResultCollector {
		private final List<RegionalInfo> data = new ArrayList<>(10);
		
		@Override
		public void collect(ResultSet resultSet) throws SQLException {
 			data.add(new RegionalInfo(
				resultSet.getInt(1),
				resultSet.getInt(2),
				resultSet.getString(3).trim(),
				resultSet.getString(4).trim(),
				resultSet.getInt(5)
			));
		}
	}
	
	@AllArgsConstructor
	@Getter
	@ToString
	public static class RegionalInfo {
		private int year;
		private int quarter;
		private String kv;
		private String name;
		private int count;
	}
	
	@Getter
	@RequiredArgsConstructor
	private static class Result {
		private final List<StatisticsEntry> overview;
		private final List<StatisticsEntry> details;
		private final Map<String, List<MorbiRsaEntry>> morbi;
		private final List<String> morbiHeader;
	}
	
	public void createStatistics(Configuration config, IOManager ioManager) {
		try (Connection conn = DriverManager.getConnection(config.getFullConnectionUrl(), config.getUser(), config.getPassword())) {
			StatisticsCalculator calculator = new StatisticsCalculator();
			RegionalStatisticCalculator regionalCalculator = new RegionalStatisticCalculator();
			List<StatisticDatasetConfig> statisticsConfigs = config.getStatistics().getDatasets();
			Path outputDirectory = ioManager.getDirectory(DirectoryType.REPORT);
			List<String> morbiHeader = extractMorbiColumnHeader(config.getStatistics().getMorbiStatisticFile());
			Map<String, Map<String, List<MorbiRsaEntry>>> insurance2MorbiStatistics = readMorbiStatistic(config.getStatistics().getMorbiStatisticFile());
			for(StatisticDatasetConfig statisticsConfig : statisticsConfigs) {
				String configDb = statisticsConfig.getDb().toLowerCase();
				String configDataset = statisticsConfig.getDataset().toLowerCase();
				if(configDb.contains("fdb")) {
					List<StatisticsEntry> overviewStatistic = calculateFDBOverviewStatistics(statisticsConfig, calculator, conn);
					Quarter referenceQuarter = findReferenceQuarter(overviewStatistic);
					List<StatisticsEntry> regionalDetails = calculateFDBDetailsStatistics(statisticsConfig, regionalCalculator, referenceQuarter, conn);
					createOutput(outputDirectory, statisticsConfig, new Result(overviewStatistic, regionalDetails, findMorbiStatisticForDataset(configDb, configDataset, insurance2MorbiStatistics), morbiHeader));
				} else if (configDb.contains("adb")) {
					List<StatisticsEntry> overviewStatistic = calculateADBOverviewStatistics(statisticsConfig, calculator, conn);
					Quarter referenceQuarter = findReferenceQuarter(overviewStatistic);
					List<StatisticsEntry> regionalDetails = calculateADBDetailsStatistics(statisticsConfig, regionalCalculator, referenceQuarter, conn);
					createOutput(outputDirectory, statisticsConfig, new Result(overviewStatistic, regionalDetails, findMorbiStatisticForDataset(configDb, configDataset, insurance2MorbiStatistics), morbiHeader));
				}
			}
			System.out.println("Done.");
		} catch (SQLException e) {
			log.error("Could not calculate statistics. ", e);
		} catch (IOException e) {
			log.error("Could not read morbi statistic. ", e);
		}
	}

	private void createOutput(Path outputDirectory, StatisticDatasetConfig settings, Result result) {
		Path file = outputDirectory.resolve("statistic_" + settings.getDataset() + ".tex");
		new LaTexOutput().createStatisticTexFile(file, result.getOverview(), result.getDetails(), result.getMorbi(), result.getMorbiHeader());
		runDocker(outputDirectory, file);
	}

	private void runDocker(Path outputDirectory, Path file) {
		Path dockerFolder = setupDockerFolder();
		try {
			SystemCall buildDocker = new SystemCall(createDockerBuildCall(dockerFolder));
			buildDocker.execute(System.out::println);
			buildDocker.awaitTermination(5, TimeUnit.MINUTES);
			runDockerLatex(outputDirectory, file);
			//long table requires second latex run
			runDockerLatex(outputDirectory, file);
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} finally {
			removeDockerFolder(dockerFolder);
		}
	}

	private void runDockerLatex(Path outputDirectory, Path file) throws IOException, InterruptedException {
		SystemCall runDocker = new SystemCall(createDockerRunCall(outputDirectory, file));
		runDocker.execute(System.out::println);
		runDocker.awaitTermination(5, TimeUnit.MINUTES);
	}

	private List<String> createDockerBuildCall(Path dockerFolder) {
		return Arrays.asList(
				"docker",
				"build",
				"-t",
				"etl-stats",
				dockerFolder.toString());
	}

	private List<String> createDockerRunCall(Path outputDirectory, Path texFile) {
		return Arrays.asList(
				"docker",
				"run",
				"--rm",
				"-v",
				outputDirectory.toAbsolutePath().toString() + ":/output",
				"etl-stats:latest",
				"-output-directory=/output",
				"/output/" + texFile.getFileName().toString()
			);
	}
	
	private void removeDockerFolder(Path dockerFolder) {
		try {
			if(Files.notExists(dockerFolder))
				return;
			Files
				.list(dockerFolder)
				.forEach(this::deleteFile);
			Files.delete(dockerFolder);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	private void deleteFile(Path file) {
		try {
			Files.delete(file);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	private Path setupDockerFolder() {
		Path folder = Paths.get(System.getProperty("user.dir")).resolve("docker-build-tmp");
		try {
			Helper.createFolders(folder);
			Files.copy(getClass().getResourceAsStream("/docker/xelatex/Dockerfile"), folder.resolve("Dockerfile"));
			Files.copy(getClass().getResourceAsStream("/logos/INGEF_Logo_ohne_claim.jpg"), folder.resolve("ingef_logo.jpg"));
		} catch (IOException e) {
			e.printStackTrace();
		}
		return folder;
	}

	private List<StatisticsEntry> calculateADBOverviewStatistics(StatisticDatasetConfig settings, StatisticsCalculator calculator, Connection connection) {
		List<StatisticsEntry> statisticsColumns = new ArrayList<>(settings.getViews().size());
		System.out.println("Calculating ADB statistics...");
		for(DataSlice slice : settings.getViews()) {
			System.out.println("\tfor '" + slice.getTable() + "'");
			String query = selectADBQueryTemplate(slice);
			query = insertADBVariables(query, settings.getH2iks(), Helper.interpolateYears(settings.getStartYear(), settings.getEndYear()));
			OverviewStatisticsCollector collector = new OverviewStatisticsCollector();
			fetchQuarterData(connection, query, collector);
			List<QuarterCount> quarterData = collector.getData();
			statisticsColumns.add(calculator.calculateOverviewStatistics(slice, quarterData, settings.getStartYear(), settings.getEndYear()));
		}
		return statisticsColumns;
	}
	
	private List<StatisticsEntry> calculateFDBOverviewStatistics(StatisticDatasetConfig settings, StatisticsCalculator calculator, Connection connection) {
		List<StatisticsEntry> statisticsColumns = new ArrayList<>(settings.getViews().size());
		System.out.println("Calculating FDB statistics...");
		for(DataSlice slice : settings.getViews()) {
			System.out.println("\tfor '" + slice.getTable() + "'");
			String query = selectFDBQueryTemplate(slice);
			query = insertFDBVariables(query, settings.getFlag(), Helper.interpolateYears(settings.getStartYear(), settings.getEndYear()));
			OverviewStatisticsCollector collector = new OverviewStatisticsCollector();
			fetchQuarterData(connection, query, collector);
			List<QuarterCount> quarterData = collector.getData();
			statisticsColumns.add(calculator.calculateOverviewStatistics(slice, quarterData, settings.getStartYear(), settings.getEndYear()));
		}
		return statisticsColumns;
	}
	
	private List<StatisticsEntry> calculateADBDetailsStatistics(StatisticDatasetConfig settings, RegionalStatisticCalculator regionalCalculator, Quarter referenceQuarter, Connection conn) {
		System.out.println("Calculating ADB details...");
		String query = Templates.Statistics.ADB_OUTPATIENT_DATA_BY_KV_QUERY.replaceAll("\\$\\{h2iks\\}", Helper.joinIks(settings.getH2iks()));
		DetailsStatisticsCollector collector = new DetailsStatisticsCollector();
		fetchQuarterData(conn, query, collector);
		List<RegionalInfo> regionalDetails = collector.getData();
		List<StatisticsEntry> regionalStatistic = new ArrayList<>(17);
		int infoIndex = 0;
		while(infoIndex < regionalDetails.size()) {
			RegionalInfo current = regionalDetails.get(infoIndex);
			if(infoIndex < regionalDetails.size() - 1) {
				RegionalInfo next = regionalDetails.get(infoIndex + 1);
				if(next.getKv().equalsIgnoreCase(current.getKv())) {
					regionalStatistic.add(
							regionalCalculator.calculateRegionalStatistic(
									referenceQuarter,
									Arrays.asList(convertToQuarterCount(current), convertToQuarterCount(next)),
									current.getKv(),
									current.getName()
							)
					);
					infoIndex += 2;
				} else {
					regionalStatistic.add(
							regionalCalculator.calculateRegionalStatistic(
									referenceQuarter,
									Arrays.asList(convertToQuarterCount(current)),
									current.getKv(),
									current.getName()									
					));
					infoIndex++;
				}
				continue;
			}
			infoIndex++;
		}
		return regionalStatistic;
	}
	
	private List<StatisticsEntry> calculateFDBDetailsStatistics(StatisticDatasetConfig settings, RegionalStatisticCalculator regionalCalculator, Quarter referenceQuarter, Connection conn) {
		System.out.println("Calculating FDB details...");
		String query = Templates.Statistics.FDB_OUTPATIENT_DATA_BY_KV_QUERY.replaceAll("\\$\\{flag\\}", settings.getFlag());
		DetailsStatisticsCollector collector = new DetailsStatisticsCollector();
		fetchQuarterData(conn, query, collector);
		List<RegionalInfo> regionalDetails = collector.getData();
		List<StatisticsEntry> regionalStatistic = new ArrayList<>(17);
		int infoIndex = 0;
		while(infoIndex < regionalDetails.size()) {
			RegionalInfo current = regionalDetails.get(infoIndex);
			if(infoIndex < regionalDetails.size() - 1) {
				RegionalInfo next = regionalDetails.get(infoIndex + 1);
				if(next.getKv().equalsIgnoreCase(current.getKv())) {
					regionalStatistic.add(
							regionalCalculator.calculateRegionalStatistic(
									referenceQuarter,
									Arrays.asList(convertToQuarterCount(current), convertToQuarterCount(next)),
									current.getKv(),
									current.getName()
							)
					);
					infoIndex += 2;
				} else {
					regionalStatistic.add(
							regionalCalculator.calculateRegionalStatistic(
									referenceQuarter,
									Arrays.asList(convertToQuarterCount(current)),
									current.getKv(),
									current.getName()									
					));
					infoIndex++;
				}
				continue;
			}
			infoIndex++;
		}
		return regionalStatistic;
	}
	
	private void fetchQuarterData(Connection conn, String query, SqlResultCollector collector) {
		try (
				PreparedStatement ps = conn.prepareStatement(query);
				ResultSet rs = ps.executeQuery();
			) {
				while(rs.next()) {
					collector.collect(rs);
				}
		} catch (SQLException e) {
			log.error("Could not fetch data. ", e);
		}
	}
	
	private String selectADBQueryTemplate(DataSlice slice) {
		switch(slice) {
		case AM_EVO:
			return Templates.Statistics.ADB_STATISTICS_FOR_AM_EVO;
		case HEMI_EVO:
		case HIMI_EVO:
			return Templates.Statistics.ADB_STATISTICS_FOR_HEMI_HIMI.replace("${tableSuffix}", slice.getTable());
		case ARZT_FALL:
			return Templates.Statistics.ADB_STATISTICS_FOR_ARZT_FALL;
		case KH_FALL:
			return Templates.Statistics.ADB_STATISTICS_FOR_KH_FALL;
		case AU_FALL:
			return Templates.Statistics.ADB_STATISTICS_FOR_AU_FALL;
		default:
			return "";
		}
	}
	
	private String selectFDBQueryTemplate(DataSlice slice) {
		switch(slice) {
		case AM_EVO:
			return Templates.Statistics.FDB_STATISTICS_FOR_AM_EVO;
		case HEMI_EVO:
		case HIMI_EVO:
			return Templates.Statistics.FDB_STATISTICS_FOR_HEMI_HIMI.replace("${tableSuffix}", slice.getTable());
		case ARZT_FALL:
			return Templates.Statistics.FDB_STATISTICS_FOR_ARZT_FALL;
		case KH_FALL:
			return Templates.Statistics.FDB_STATISTICS_FOR_KH_FALL;
		case AU_FALL:
			return Templates.Statistics.FDB_STATISTICS_FOR_AU_FALL;
		default:
			return "";
		}
	}
	
	private String insertADBVariables(String queryTemplate, List<String> h2iks, List<Integer> years) {
		String combinedH2iks = Helper.joinIks(h2iks);
		String combinedYears = years.stream().map(i -> Integer.toString(i)).collect(Collectors.joining(","));
		String finalQuery = queryTemplate.replaceAll("\\$\\{h2iks\\}", combinedH2iks);
		finalQuery = finalQuery.replaceAll("\\$\\{years\\}", combinedYears);
		return finalQuery;
	}
	
	private String insertFDBVariables(String queryTemplate, String flag, List<Integer> years) {
		String combinedYears = years.stream().map(i -> Integer.toString(i)).collect(Collectors.joining(","));
		String finalQuery = queryTemplate.replaceAll("\\$\\{flag\\}", flag);
		finalQuery = finalQuery.replaceAll("\\$\\{years\\}", combinedYears);
		return finalQuery;
	}
	
	private QuarterCount convertToQuarterCount(RegionalInfo regionalInfo) {
		Quarter quarter = new Quarter(regionalInfo.getYear(), regionalInfo.getQuarter());
		return new QuarterCount(quarter, regionalInfo.getCount());
	}
	
	private Quarter findReferenceQuarter(List<StatisticsEntry> statisticColumns) {
		Optional<Quarter> maxQuarter = statisticColumns
			.stream()
			.filter(entry -> entry.getLabel().equalsIgnoreCase(DataSlice.ARZT_FALL.getLabel()))
			.flatMap(entry -> entry.getDataCount().stream())
			.filter(quarterCount -> quarterCount.getCount() > 0)
			.map(quarterCount -> quarterCount.getQuarter())
			.collect(
					Collectors.maxBy(
							Comparator.comparing(Quarter::getYear).thenComparing(Quarter::getQuarter)
					)
			);

		return maxQuarter.get();
	}
	
	private List<String> extractMorbiColumnHeader(Path morbiStatisticFile) throws IOException {
		BufferedReader reader = Files.newBufferedReader(morbiStatisticFile);		
		String[] header = reader.readLine().split(";");
		reader.close();
		//drop insurance name and setup
		return IntStream
			.range(2, header.length)
			.mapToObj(column -> header[column])
			.collect(Collectors.toList());
	}
	
	private Map<String, Map<String, List<MorbiRsaEntry>>> readMorbiStatistic(Path morbiStatisticFile) throws IOException {
		BufferedReader reader = Files.newBufferedReader(morbiStatisticFile);
		return reader
				.lines()
				.skip(1)
				.filter(line -> line != null && !line.isEmpty())
				.map(line -> line.split(";"))
				.map(columns -> new MorbiRsaEntry(columns[0], columns[1], Integer.parseInt(columns[2]), Integer.parseInt(columns[3]), Integer.parseInt(columns[4]), columns[5], columns[6]))
				.collect(Collectors.groupingBy(MorbiRsaEntry::getInsurance, Collectors.groupingBy(MorbiRsaEntry::getSetup)));
	}

	/**
	 * finds the associated morbi rsa entries for the given db or dataset
	 * @param db partially matched against map keys
	 * @param dataset partially matched against map keys
	 * @param statistic mapping of insurance name onto setup onto configurations
	 * @return empty list if no key matched
	 */
	private Map<String, List<MorbiRsaEntry>> findMorbiStatisticForDataset(String db, String dataset, Map<String, Map<String, List<MorbiRsaEntry>>> statistic) {
		for(String insurance : statistic.keySet()) {
			String normalizedName = insurance.toLowerCase();
			if(normalizedName.contains(db) || normalizedName.contains(dataset))
				return statistic.get(insurance);
		}
		return Collections.emptyMap();
	}
}