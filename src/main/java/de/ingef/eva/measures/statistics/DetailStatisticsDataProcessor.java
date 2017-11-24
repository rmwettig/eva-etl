package de.ingef.eva.measures.statistics;

import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import de.ingef.eva.data.DataTable;
import de.ingef.eva.data.RowElement;
import de.ingef.eva.data.SimpleRowElement;
import de.ingef.eva.data.TeradataColumnType;
import de.ingef.eva.datasource.DataProcessor;
import de.ingef.eva.datasource.inmemory.InMemoryDataTable;
import de.ingef.eva.error.DataTableOperationException;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.log4j.Log4j2;

@Log4j2
public class DetailStatisticsDataProcessor implements DataProcessor {
	
	@AllArgsConstructor
	@Getter
	private class RegionalInfo {
		private int year;
		private int quarter;
		private int kv;
		private String name;
		private int count;
	}
	
	/**
	 * creates a data table with regional details.
	 * 
	 * The expected structure of the input is as follows:
	 * There must be one row with the data for the latest year and quarter and one with the previous year.
	 * Rows must be ordered descendingly by year and kv.
	 * Singular rows, i.e. no data for the current year, are displayed with 0 in the output. 
	 * 
	 * Example:
	 * year quarter kv    kv_name		  	  count			
	 * 2015	  4     01	 Schleswig-Holstein	  1256
	 * 2016	  4	    01	 Schleswig-Holstein   1287
	 * 2015	  4	    02	 Hamburg              1832
	 * 2016	  4	    03	 Bremen               361
	 * 2015	  4	    03	 Bremen               350
	 *
	 * @return 
	 */
	@Override
	public DataTable process(DataTable... dataTables) {
		DataTable table = dataTables[0];
		
		return new InMemoryDataTable(table.getName(), createDetailsTableRows(table).listIterator(), createHeader());
	}

	private List<RowElement> createHeader() {
		List<RowElement> header = new ArrayList<>(5);
		header.add(new SimpleRowElement("Quartal", 0, TeradataColumnType.CHARACTER, "Quartal"));
		header.add(new SimpleRowElement("KV", 1, TeradataColumnType.CHARACTER, "KV"));
		header.add(new SimpleRowElement("KV_NAME", 2, TeradataColumnType.CHARACTER, "KV_NAME"));
		header.add(new SimpleRowElement("Anzahl Fälle", 3, TeradataColumnType.INTEGER, "Anzahl Fälle"));
		header.add(new SimpleRowElement("Anteil VJQ", 4, TeradataColumnType.FLOAT, "Anteil VJQ"));
		
		return header;
	}
	
	private List<List<RowElement>> createDetailsTableRows(DataTable table) {
		List<List<RowElement>> rows = new ArrayList<>();
		try {
			NumberFormat numberFormatter = NumberFormat.getNumberInstance(Locale.GERMAN);
			table.open();
			RegionalInfo lastInfo = null;
			
			while(table.hasMoreRows()) {
				List<RowElement> row = table.getNextRow(true);
				if(lastInfo == null) {
					lastInfo = createRegionalInfo(row);
				} else {
					RegionalInfo current = createRegionalInfo(row);
					//new data for a kv was found
					if(lastInfo.getKv() == current.getKv()) {
						rows.add(createDetailsRow(lastInfo, current, numberFormatter));
						lastInfo = null;
					} else {
						rows.add(createDetailsNotFoundRow(lastInfo));
						lastInfo = current;
					}
				}
			}
			table.close();	
		} catch(DataTableOperationException e) {
			log.error(e.getMessage());
		}
		
		return rows;
	}
	
	private RegionalInfo createRegionalInfo(List<RowElement> row) {
		return new RegionalInfo(
					Integer.parseInt(row.get(0).getContent()),
					Integer.parseInt(row.get(1).getContent()),
					Integer.parseInt(row.get(2).getContent()),
					row.get(3).getContent(),
					Integer.parseInt(row.get(4).getContent())
				);
	}
	
	/**
	 * creates the per-KV changes including ratio 
	 * @param first
	 * @param second
	 * @param formatter
	 * @return
	 */
	private List<RowElement> createDetailsRow(RegionalInfo first, RegionalInfo second, NumberFormat formatter) {
		List<RowElement> row = new ArrayList<>(5);
		RegionalInfo latest = second; 
		RegionalInfo previous = first;
		if(latest.getYear() < previous.getYear()){
			latest = first;
			previous = second;
		}
		
		row.add(new SimpleRowElement("Quartal", 0, TeradataColumnType.CHARACTER, latest.getYear() + "Q" + latest.getQuarter()));
		row.add(new SimpleRowElement("KV", 1, TeradataColumnType.CHARACTER, latest.getKv() < 10 ? "0" + latest.getKv() : Integer.toString(latest.getKv())));
		row.add(new SimpleRowElement("Name", 2, TeradataColumnType.CHARACTER, latest.getName()));
		row.add(new SimpleRowElement("Anzahl Fälle", 3, TeradataColumnType.INTEGER, formatter.format(latest.getCount())));
		row.add(new SimpleRowElement("Anteil VJQ", 4, TeradataColumnType.FLOAT, formatter.format(latest.getCount() / (float) previous.getCount())));
				
		return row;
	}
	
	private List<RowElement> createDetailsNotFoundRow(RegionalInfo last) {
		List<RowElement> row = new ArrayList<>(5);
		row.add(new SimpleRowElement("Quartal", 0, TeradataColumnType.CHARACTER, last.getYear() + 1 + "Q" + last.getQuarter()));
		row.add(new SimpleRowElement("KV", 1, TeradataColumnType.CHARACTER, last.getKv() < 10 ? "0" + last.getKv() : Integer.toString(last.getKv())));
		row.add(new SimpleRowElement("Name", 2, TeradataColumnType.CHARACTER, last.getName()));
		row.add(new SimpleRowElement("Anzahl", 3, TeradataColumnType.INTEGER, "0"));
		row.add(new SimpleRowElement("Ratio", 4, TeradataColumnType.FLOAT, "0"));
				
		return row;
	}
}
