package de.ingef.eva.dataprocessor;

import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import de.ingef.eva.data.DataTable;
import de.ingef.eva.data.RowElement;
import de.ingef.eva.data.SimpleRowElement;
import de.ingef.eva.data.TeradataColumnType;
import de.ingef.eva.datasource.DataProcessor;
import de.ingef.eva.datasource.inmemory.InMemoryDataTable;
import de.ingef.eva.error.DataTableOperationException;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;

/**
 * Aggregates data amount changes for different tables
 * 
 * @author Martin Wettig
 *
 */
@Log4j2
public class StatisticsDataProcessor implements DataProcessor {

	@RequiredArgsConstructor
	@Getter
	private class YearQuarterCount {
		private final int year;
		private final int quarter;
		private final int count;
	}
	
	@RequiredArgsConstructor
	@Getter
	private class CountRatio {
		private final int count;
		private final float ratio;
	}
	
	/**
	 * takes a single data table and creates a data table with two columns.
	 * The first contains the year-quarter, the second absolute count and ratio
	 */
	@Override
	public DataTable process(DataTable... dataTables) {
		//maps year-quarter to row entries
		Map<String, CountRatio[]> deltaTable = new HashMap<>();
		int tableCount = dataTables.length;
		String[] header = new String[tableCount];
		//all tables for which data statistics should be calculated
		for(int i = 0; i < tableCount; i++) {
			DataTable table = dataTables[i];
			header[i] = table.getName();
			List<YearQuarterCount> counts = convertDataTableToList(table);
			Map<String, SimpleEntry<Integer, Float>> deltas = createDeltaAggregation(counts);
			for(String yearQuarter : deltas.keySet()) {
				//year-quarter pair was already added
				if(deltaTable.containsKey(yearQuarter)) {
					SimpleEntry<Integer,Float> stats = deltas.get(yearQuarter);
					//add the delta entry for the current data set
					CountRatio countInfo = new CountRatio(stats.getKey(), stats.getValue());
					deltaTable.get(yearQuarter)[i] = countInfo;
				} else {
					//create a new delta entry
					CountRatio[] row = new CountRatio[tableCount];
					SimpleEntry<Integer,Float> stats = deltas.get(yearQuarter);
					row[i] = new CountRatio(stats.getKey(), stats.getValue());
					deltaTable.put(yearQuarter, row);
				}
			}
		}
		
		//+1 because of quarter column
		return createStatisticsTable(deltaTable, header, dataTables.length + 1, dataTables[0].getName());
	}
	
	private List<YearQuarterCount> convertDataTableToList(DataTable table) {
		List<YearQuarterCount> counts = new ArrayList<>();
		try {
			table.open();
			while(table.hasMoreRows()) {
				List<RowElement> row = table.getNextRow(true);
				int year = Integer.parseInt(row.get(0).getContent());
				int quarter = Integer.parseInt(row.get(1).getContent());
				int count = Integer.parseInt(row.get(2).getContent());
				counts.add(new YearQuarterCount(year, quarter, count));
			}
			table.close();
		} catch(DataTableOperationException e) {
			log.error(e);
		}
		
		return counts;
	}
	
	/**
	 * takes ordered data table counts and creates the ratio information 
	 * @param counts
	 * @return column representation
	 */
	private Map<String,SimpleEntry<Integer,Float>> createDeltaAggregation(List<YearQuarterCount> counts) {
		// maps year-quarters to entry counts and set size change
		Map<String,SimpleEntry<Integer,Float>> deltas = new LinkedHashMap<String,SimpleEntry<Integer,Float>>();
		// an ordered list is expected
		// therefore, the first four rows correspond to entries w/o values for the last year
		for(int i = 0; i < 4; i++) {
			YearQuarterCount t = counts.get(i);
			String yearQuarter = t.getYear() + "Q" + t.getQuarter();
			deltas.put(yearQuarter, new SimpleEntry<>(t.getCount(), 0f));
		}
		for(int i = 4; i < counts.size(); i++) {
			YearQuarterCount lastYear = counts.get(i - 4);
			YearQuarterCount currentYear = counts.get(i);
			String yearQuarter = currentYear.getYear() + "Q" + currentYear.getQuarter();
			deltas.put(yearQuarter, new SimpleEntry<>(currentYear.getCount(), currentYear.getCount()/(float)lastYear.getCount()));
		}
		
		return deltas;
	}
	
	/**
	 * creates the final statistics for all tables including change ratios
	 * @param quarterCounts yearQuarter string -> dataset name -> (absolute number, ratio)
	 * @return
	 */
	private DataTable createStatisticsTable(Map<String, CountRatio[]> quarterCounts, String[] columnNames, int columnCount, String name) {
		List<List<RowElement>> rows = new ArrayList<>(100);
		List<RowElement> header = new ArrayList<>(columnCount);
		header.add(new SimpleRowElement("Quartal", 0, TeradataColumnType.CHARACTER, "Quartal"));
		for(int i = 0; i < columnNames.length; i++)
			header.add(new SimpleRowElement(columnNames[i], i, TeradataColumnType.CHARACTER, columnNames[i]));

		NumberFormat nf = NumberFormat.getNumberInstance(Locale.GERMAN);
		DecimalFormat df = new DecimalFormat("##0.0");
		
		for(String yearQuarter : quarterCounts.keySet()) {
			List<RowElement> row = new ArrayList<>(columnCount);
			row.add(new SimpleRowElement("Quartal", 0, TeradataColumnType.CHARACTER, yearQuarter));
			CountRatio[] rowEntries = quarterCounts.get(yearQuarter);
			for(int i = 0; i < rowEntries.length; i++) {
				CountRatio stats = rowEntries[i];
				if(stats != null) {
					float ratio = stats.getRatio();
					String absoluteCount = nf.format(stats.getCount());
					String content = ratio > 0 ? absoluteCount + " (" + df.format(ratio) + ")" : absoluteCount;
					row.add(new SimpleRowElement(columnNames[i], row.size(), TeradataColumnType.CHARACTER, content));
				} else {
					row.add(new SimpleRowElement(columnNames[i], row.size(), TeradataColumnType.CHARACTER, "0"));
				}
			}
			rows.add(row);
		}
		
		//make sure that earliest year is first
		rows.sort(new Comparator<List<RowElement>>() {
			@Override
			public int compare(List<RowElement> o1, List<RowElement> o2) {
				return o1.get(0).getContent().compareTo(o2.get(0).getContent());
			}
		});
		
		return new InMemoryDataTable(name, rows.listIterator(), header);
	}

}
