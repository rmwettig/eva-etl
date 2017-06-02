package de.ingef.eva.dataprocessor;

import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import de.ingef.eva.datasource.DataProcessor;
import de.ingef.eva.datasource.DataTable;
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

	private List<String> createHeader() {
		List<String> header = new ArrayList<>(5);
		header.add("Quartal");
		header.add("KV");
		header.add("KV_NAME");
		header.add("Anzahl Fälle");
		header.add("Anteil VJQ");
		
		return header;
	}
	
	private List<String[]> createDetailsTableRows(DataTable table) {
		List<String[]> rows = new ArrayList<>();
		try {
			NumberFormat numberFormatter = NumberFormat.getNumberInstance(Locale.GERMAN);
			table.open();
			RegionalInfo lastInfo = null;
			
			while(table.hasMoreRows()) {
				String[] row = table.getNextRow();
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
	
	private RegionalInfo createRegionalInfo(String[] row) {
		return new RegionalInfo(
					Integer.parseInt(row[0]),
					Integer.parseInt(row[1]),
					Integer.parseInt(row[2]),
					row[3],
					Integer.parseInt(row[4])
				);
	}
	
	/**
	 * creates the per-KV changes including ratio 
	 * @param first
	 * @param second
	 * @param formatter
	 * @return
	 */
	private String[] createDetailsRow(RegionalInfo first, RegionalInfo second, NumberFormat formatter) {
		String row[] = new String[5];
		RegionalInfo latest = second; 
		RegionalInfo previous = first;
		if(latest.getYear() < previous.getYear()){
			latest = first;
			previous = second;
		}
		
		row[0] = latest.getYear() + "Q" + latest.getQuarter();
		row[1] = latest.getKv() < 10 ? "0" + latest.getKv() : Integer.toString(latest.getKv());
		row[2] = latest.getName();
		row[3] = formatter.format(latest.getCount());
		row[4] = formatter.format(latest.getCount() / (float) previous.getCount());
				
		return row;
	}
	
	private String[] createDetailsNotFoundRow(RegionalInfo last) {
		String row[] = new String[5];
		row[0] = last.getYear()+1+"Q"+last.getQuarter();
		row[1] = last.getKv() < 10 ? "0" + last.getKv() : Integer.toString(last.getKv());
		row[2] = last.getName();
		row[3] = "0";
		row[4] = "0";
		
		return row;
	}
}
