package de.ingef.eva.mapping;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import de.ingef.eva.configuration.Configuration;
import de.ingef.eva.datasource.DataProcessor;
import de.ingef.eva.datasource.DataTable;
import de.ingef.eva.datasource.file.FileDataTable;
import de.ingef.eva.error.DataTableOperationException;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.extern.log4j.Log4j2;

@Log4j2 @RequiredArgsConstructor
public class ProcessPidDecode implements DataProcessor {
	
	private final Configuration config;
	
	@AllArgsConstructor
	private static class Mapping {
		@Getter
		private final String h2ik;
		@Getter @Setter
		private String egknr;
		@Getter @Setter
		private String kvNr;
		@Getter
		private final String pid;
	}
	
	/**
	 * Creates a pid decode mapping
	 * @param dataTables expects two data tables, the first contains the raw, unfiltered pids, the second contains the pids to be excluded
	 * @return a pid mapping data table
	 */
	@Override
	public DataTable process(DataTable... dataTables) {
		DataTable unfilteredPids = dataTables[0];
		Set<String> pidFilter = createFilterPids(dataTables[1]);
		Map<String,Mapping> cleanPidMappings = removeDuplicates(unfilteredPids, pidFilter);		
				
		try {
			return createMappingDataTable(cleanPidMappings, unfilteredPids.getName(), unfilteredPids.getColumnNames());
		} catch (DataTableOperationException e) {
			log.error(e.getStackTrace()[0]);
		}
		
		return null;
	}
	
	private Set<String> createFilterPids(DataTable pidTable) {
		Set<String> pids = new HashSet<String>();
		try {
			pidTable.open();
			while(pidTable.hasMoreRows()) {
				pids.add(pidTable.getNextRow()[0]);
			}
			pidTable.close();
		} catch (DataTableOperationException e) {
			log.error("Could not create pid filter.\nReason: {}.\nStackTrace: {}", e.getMessage(), e.getStackTrace());
		}
		
		return pids;
	}
	
	/**
	 * adds zeros to pad the pid to length 10 if pid does not start with zeros
	 * @param pid
	 * @return
	 */
	private String addPaddingZeros(String pid) {
		if(pid.startsWith("0"))
			return pid;
		StringBuilder sb = new StringBuilder(10);
		int paddingLength = 10 - pid.length();
		for(int i = 0; i < paddingLength; i++)
			sb.insert(i, '0');
		sb.insert(paddingLength, pid);
		
		return sb.toString();
	}
	
	/**
	 * creates unique mappings with spread information combined
	 * @param rawPids unfiltered pids
	 * @param unwantedPids pids that must be removed from the set
	 * @return h2ikToPid map
	 */
	private Map<String,Mapping> removeDuplicates(DataTable rawPids, Set<String> unwantedPids) {
		try {
			Map<String, Mapping> mappings = new HashMap<>();
			Set<String> uniquePids = new HashSet<>();
			rawPids.open();
			while(rawPids.hasMoreRows()) {
				String[] row = rawPids.getNextRow();
				String pid = row[row.length - 1];
				//skip entries with empty h2ik, pid
				if(row[0] == null || row[0].isEmpty() || pid == null || pid.isEmpty()) continue;
				pid = addPaddingZeros(pid);
				uniquePids.add(pid);
				//skip unwanted pids
				if(unwantedPids.contains(pid)) continue;
				String key = row[0] + pid;
				if(!mappings.containsKey(key))
					mappings.put(key, new Mapping(row[0], row[1], row[2], pid));
				else {
					Mapping m = mappings.get(key);
					//update egk and kv number if it was an invalid value before only
					if(m.getEgknr() == null || m.getEgknr().isEmpty())
						m.setEgknr(row[1]);
					if(m.getKvNr() == null || m.getKvNr().isEmpty())
						m.setKvNr(row[2]);
				}
			}
			rawPids.close();
			calculateLoss(uniquePids, mappings.keySet());
						
			return mappings;
		} 
		catch (DataTableOperationException e) {
			log.error(e.getStackTrace()[0]);
		}
		
		return null;
	}
	
	private void calculateLoss(Set<String> uniquePids, Set<String> keySet) {
		float lossPercentage = keySet.size() / (float)uniquePids.size();
		if(lossPercentage > 0.05f)
			log.warn("PID loss: {}", lossPercentage*100);
	}

	private DataTable createMappingDataTable(Map<String,Mapping> mappings, String name, List<String> columnNames) {
		File outfile = new File(config.getOutDirectory() + "/decoding_" + name + ".csv");
		try (BufferedWriter writer = new BufferedWriter(new FileWriter(outfile))) {
			StringBuilder header = new StringBuilder();
			for(String columnName : columnNames) {
				header.append(columnName);
				header.append(";");
			}
			header.deleteCharAt(header.lastIndexOf(";"));
			writer.write(header.toString());
			writer.write('\n');
			for(Mapping m : mappings.values()) {
				String row = String.format("%s;%s;%s;%s", m.getH2ik(), m.getEgknr(), m.getKvNr(), m.getPid());
				writer.write(row);
				writer.write('\n');
			}
		} catch (IOException e) {
			log.error("Could not create mapping file.\nReason: {}", e.getMessage());
		}
		
		return new FileDataTable(outfile, ";", name);
	}
	
}
