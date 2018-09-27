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
import java.util.stream.Collectors;

import de.ingef.eva.constant.OutputDirectory.DirectoryType;
import de.ingef.eva.data.DataTable;
import de.ingef.eva.data.RowElement;
import de.ingef.eva.datasource.DataProcessor;
import de.ingef.eva.datasource.file.FileDataTable;
import de.ingef.eva.error.DataTableOperationException;
import de.ingef.eva.utility.Helper;
import de.ingef.eva.utility.io.IOManager;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.extern.log4j.Log4j2;

@Log4j2 @RequiredArgsConstructor
public class ProcessPidDecode implements DataProcessor {
	
	private final IOManager ioManager;
	private static final int EXPECTED_PID_LENGTH = 10;	
	
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
	
	/**
	 * Creates a pid set from data table.
	 * @param pidTable will be closed after use
	 * @return
	 */
	private Set<String> createFilterPids(DataTable pidTable) {
		Set<String> pids = new HashSet<String>();
		try {
			pidTable.open();
			while(pidTable.hasMoreRows()) {
				pids.add(pidTable.getNextRow(true).get(0).getContent());
			}
			pidTable.close();
		} catch (DataTableOperationException e) {
			log.error("Could not create pid filter.\nReason: {}.\nStackTrace: {}", e.getMessage(), e.getStackTrace());
		}
		
		return pids;
	}
	
	/**
	 * creates unique mappings with spread information combined
	 * @param rawPids unfiltered pids. Will be closed after use
	 * @param unwantedPids pids that must be removed from the set
	 * @return h2ikToPid map
	 */
	private Map<String,Mapping> removeDuplicates(DataTable rawPids, Set<String> unwantedPids) {
		try {
			Map<String, Mapping> ikPid2Mapping = new HashMap<>();
			Set<String> uniquePids = new HashSet<>();
			Set<String> uniqueEgkNumbers = new HashSet<>();
			Set<String> uniqueKvNumbers = new HashSet<>();
			int numberOfDuplicates = 0;
			int numberOfValidMappings = 0;
			rawPids.open();
			while(rawPids.hasMoreRows()) {
				List<RowElement> row = rawPids.getNextRow(true);
				String pid = row.get(row.size() - 1).getContent();
				String h2ik = row.get(0).getContent();
				//skip entries with empty h2ik, pid
				if(h2ik == null || h2ik.isEmpty() || pid == null || pid.isEmpty()) continue;
				pid = Helper.addPaddingZeros(pid, EXPECTED_PID_LENGTH);
				//skip unwanted pids
				if(unwantedPids.contains(pid)) continue;
				if(uniquePids.contains(pid)) {
					log.warn("Found duplicated pid: '{}'. Entry: [{}]", pid, rowToString(row));
					numberOfDuplicates++;
					continue;
				}
				uniquePids.add(pid);
				
				String egkNo = row.get(1).getContent();
				
				if(egkNo != null && !egkNo.isEmpty() && uniqueEgkNumbers.contains(egkNo)) {
					log.warn("Found duplicated egk number: '{}'. Entry: [{}]", egkNo, rowToString(row));
					numberOfDuplicates++;
					continue;
				}
				uniqueEgkNumbers.add(egkNo);
				
				String kvNo = row.get(2).getContent();
				if(kvNo != null && !kvNo.isEmpty() && uniqueKvNumbers.contains(kvNo)) {
					log.warn("Found duplicated kv_number: '{}', Entry: [{}]", kvNo, rowToString(row));
					numberOfDuplicates++;
					continue;
				}
				uniqueKvNumbers.add(kvNo);
				
				//save first occurrence to columns as key if egk is not empty
				//increase counter for each time the key was found -> report dup
				String key = h2ik + pid;
				if(!ikPid2Mapping.containsKey(key)) {
					ikPid2Mapping.put(key, new Mapping(h2ik, egkNo, kvNo, pid));
					numberOfValidMappings++;
				} else {
					log.warn("Found duplicated ik+pid key: '{}'. Entry: [{}]", key, rowToString(row));
					numberOfDuplicates++;
				}
			}
			rawPids.close();
			calculateLoss(numberOfDuplicates, numberOfValidMappings);
						
			return ikPid2Mapping;
		} 
		catch (DataTableOperationException e) {
			log.error(e.getStackTrace()[0]);
		}
		
		return null;
	}
	
	private void calculateLoss(int numberOfDuplicates, int numberOfValidMappings) {
		float loss =  numberOfDuplicates / (float)numberOfValidMappings;
		if(loss > 0.05f)
			log.warn("PID loss: {}", loss * 100);
	}

	private DataTable createMappingDataTable(Map<String,Mapping> mappings, String name, List<RowElement> columnNames) {
		File outfile = ioManager.getDirectory(DirectoryType.PRODUCTION).resolve("decoding_" + name + ".csv").toFile();
		try (BufferedWriter writer = new BufferedWriter(new FileWriter(outfile))) {
			StringBuilder header = new StringBuilder();
			for(RowElement columnName : columnNames) {
				header.append(columnName.getContent());
				header.append(";");
			}
			header.deleteCharAt(header.lastIndexOf(";"));
			writer.write(header.toString());
			writer.write('\n');
			for(Mapping m : mappings.values()) {
				String kvNr = (m.getEgknr() == null || m.getEgknr().isEmpty()) ? m.getKvNr() : "";
				String row = String.format("%s;%s;%s;%s", m.getH2ik(), m.getEgknr(), kvNr, m.getPid());
				writer.write(row);
				writer.write('\n');
			}
		} catch (IOException e) {
			log.error("Could not create mapping file.\nReason: {}", e.getMessage());
		}
		
		//TODO solve unused header in a better way
		return new FileDataTable(outfile, ";", name, columnNames);
	}
	
	private String rowToString(List<RowElement> row) {
		return row.stream().map(e -> e.getContent()).collect(Collectors.joining(", "));
	}
}
