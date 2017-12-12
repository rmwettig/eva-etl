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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import de.ingef.eva.configuration.Configuration;
import de.ingef.eva.configuration.statistics.StatisticDatasetConfig;
import de.ingef.eva.constant.Templates;
import de.ingef.eva.measures.cci.Quarter;
import de.ingef.eva.utility.Helper;
import de.ingef.eva.utility.QuarterCount;
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
		private final List<MorbiRsaEntry> morbi;
		private final List<String> morbiHeader;
	}
	
	public void createStatistics(Configuration config) {
		try (Connection conn = DriverManager.getConnection(config.getFullConnectionUrl(), config.getUser(), config.getPassword())) {
			StatisticsCalculator calculator = new StatisticsCalculator();
			RegionalStatisticCalculator regionalCalculator = new RegionalStatisticCalculator();
			List<StatisticDatasetConfig> statisticsConfigs = config.getStatistics().getDatasets();
			String outputDirectory = config.getOutputDirectory();
			List<String> morbiHeader = extractMorbiColumnHeader(config.getStatistics().getMorbiStatisticFile());
			Map<String, List<MorbiRsaEntry>> insurance2MorbiStatistics = readMorbiStatistic(config.getStatistics().getMorbiStatisticFile());
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
		
	private void createOutput(String outputDirectory, StatisticDatasetConfig settings, Result result) {
		Path file = Paths.get(outputDirectory, settings.getDb(), settings.getDataset());
		try {
			Helper.createFolders(file);
		} catch (IOException e) {
			log.error("Could not create path: '{}'. ", file.toString(), e);
		}
		file = file.resolve("statistic.pdf");
		new StatisticPdfOutput().createStatisticOutput(file, result.getOverview(), result.getDetails(), result.getMorbi(), result.getMorbiHeader());
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
		//insurance name is not important so just drop it
		return IntStream
			.range(1, header.length)
			.mapToObj(column -> header[column])
			.collect(Collectors.toList());
	}
	
	private Map<String, List<MorbiRsaEntry>> readMorbiStatistic(Path morbiStatisticFile) throws IOException {
		BufferedReader reader = Files.newBufferedReader(morbiStatisticFile);
		return reader
				.lines()
				.skip(1)
				.filter(line -> line != null && !line.isEmpty())
				.map(line -> line.split(";"))
				.map(columns -> new MorbiRsaEntry(columns[0], Integer.parseInt(columns[1]), Integer.parseInt(columns[2]), Integer.parseInt(columns[3]), columns[4], columns[5]))
				.collect(Collectors.groupingBy(MorbiRsaEntry::getInsurance));
	}
	
	private List<MorbiRsaEntry> findMorbiStatisticForDataset(String db, String dataset, Map<String, List<MorbiRsaEntry>> statistic) {
		return statistic
				.entrySet()
				.stream()
				.filter(entry -> entry.getKey().toLowerCase().contains(dataset) || entry.getKey().toLowerCase().contains(db))
				.flatMap(entry -> entry.getValue().stream())
				.collect(Collectors.toList());
	}
}


