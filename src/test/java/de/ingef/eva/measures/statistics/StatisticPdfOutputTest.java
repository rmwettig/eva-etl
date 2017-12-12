package de.ingef.eva.measures.statistics;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;

import org.junit.Test;

import de.ingef.eva.measures.cci.Quarter;
import de.ingef.eva.utility.Helper;
import de.ingef.eva.utility.QuarterCount;

public class StatisticPdfOutputTest {

	@Test
	public void visualLayoutCheck() throws IOException {
		Path path = Paths.get("out/pdfLayoutTest");
		Helper.createFolders(path);
		path = path.resolve("statistic.pdf");
		new StatisticPdfOutput().createStatisticOutput(path, createOverview(), createDetails(), createMorbi(), createMorbiHeader());
	}
	
	private List<MorbiRsaEntry> createMorbi() {
		return Arrays.asList(
				new MorbiRsaEntry("Bosch BKK", 2017, 2010, 2011, "Q1-Q4", "Grundlagenbescheid 2"),
				new MorbiRsaEntry("Bosch BKK", 2017, 2011, 2012, "Q1-Q4", "Grundlagenbescheid 2"),
				new MorbiRsaEntry("Bosch BKK", 2017, 2012, 2013, "Q1-Q4", "Grundlagenbescheid 2"),
				new MorbiRsaEntry("Bosch BKK", 2017, 2013, 2014, "Q1-Q4", "Grundlagenbescheid 2"),
				new MorbiRsaEntry("Bosch BKK", 2017, 2014, 2015, "Q1-Q4", "Grundlagenbescheid 2")
		);
	}

	private List<String> createMorbiHeader() {
		return Arrays.asList("Grouperjahr", "Leistungsjahr", "Berichtsjahr", "Quartal", "Konfiguration");
	}
	
	private List<StatisticsEntry> createOverview() {
		Random random = new Random(3028);
		return Arrays.asList(DataSlice.ARZT_FALL, DataSlice.AM_EVO, DataSlice.KH_FALL, DataSlice.HEMI_EVO, DataSlice.HIMI_EVO, DataSlice.AU_FALL)
			.stream()
			.map(slice -> {
				List<QuarterCount> dataCount = createPseudoData(random);
				return new StatisticsEntry(slice.getLabel(), slice.getLabel(), dataCount);
			})
			.collect(Collectors.toList());
	}
	
	private List<QuarterCount> createPseudoData(Random random) {
		List<QuarterCount> data = new ArrayList<>(8);
		Quarter quarter = new Quarter(9998, 1);
		for(int i = 0; i < 8; i++) {
			int number = random.nextInt(1_000_000) + 1_000_000;
			if(i < 4) {
				data.add(new QuarterCount(quarter, number));
			} else {
				QuarterCount count = new QuarterCount(quarter, number);
				count.setChangeRatio(count.getCount() / (double)data.get(i - 4).getCount());
				data.add(count);
			}
			quarter = quarter.increment();
		}
		
		return data;
	}
	
	private List<StatisticsEntry> createDetails() {
		List<StatisticsEntry> details = new ArrayList<>(2);
		details.add(new StatisticsEntry("Schleswig-Holstein", "01", Arrays.asList(new QuarterCount(new Quarter(9999, 2), 2300))));
		QuarterCount count = new QuarterCount(new Quarter(9999, 2), 23300);
		count.setChangeRatio(1.8);
		details.add(new StatisticsEntry("Thüringen", "93", Arrays.asList(count)));
		QuarterCount countBaWue = new QuarterCount(new Quarter(9999, 2), 230);
		countBaWue.setChangeRatio(0.2);
		details.add(new StatisticsEntry("Baden-Württemberg", "52", Arrays.asList(countBaWue)));
		
		return details;
	}
}
