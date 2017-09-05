package de.ingef.eva.dataprocessor;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import de.ingef.eva.constant.OutputDirectory;
import de.ingef.eva.data.DataTable;
import de.ingef.eva.data.RowElement;
import de.ingef.eva.datasource.DataProcessor;
import de.ingef.eva.error.DataTableOperationException;
import de.ingef.eva.utility.CsvWriter;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;

@Log4j2
@RequiredArgsConstructor
public class DynamicColumnAppender implements DataProcessor {

	private final String keyColumnName;
	private final String datasetName;
	private final String outputFolder;
	
	@Override
	public DataTable process(DataTable... dataTables) {
		DataTable main = null;
		DataTable additionalColumns = null;
		CsvWriter writer = null;
		
		Path p = Paths.get(outputFolder, OutputDirectory.PRODUCTION, datasetName);
		if(!Files.exists(p)) {
			try {
				Files.createDirectories(p);
			} catch (IOException e) {
				log.error("Could not create directory '{}'. {}", p.toString(), e);
			}
		}
		
		try {
			main = dataTables[0];
			additionalColumns = dataTables[1];
			main.open();
			additionalColumns.open();
			writer = new CsvWriter(p.resolve(main.getName()).toFile());
			writer.open();
			List<RowElement> extendedHeader = createExtendedHeader(main.getColumnNames(), additionalColumns.getColumnNames());
			for(RowElement e : extendedHeader)
				writer.addEntry(e.getContent());
			writer.writeLine();
			
			int mainKeyColumnIndex = findKeyColumnInTable(main.getColumnNames());
			if(mainKeyColumnIndex == -1) {
				log.error("Missing key column '{}' in table '{}'", keyColumnName, main.getName());
				return null;
			}

			Map<String,List<RowElement>> key2content = createContentMap(additionalColumns);
			//remove main header
			if(main.hasMoreRows())
				main.getNextRow(true);
			while(main.hasMoreRows()) {
				List<RowElement> row = main.getNextRow(true);
				for(RowElement e : row) {
					writer.addEntry(e.getContent());
				}
				String mainKey = row.get(mainKeyColumnIndex).getContent();
				if(key2content.containsKey(mainKey)) {
					for(RowElement e : key2content.get(mainKey)) {
						writer.addEntry(e.getContent());
					}
				} else {
					//add padding for rows without additional data
					for(int i = 0; i < additionalColumns.getColumnNames().size() - 1; i++) {
						writer.addEntry("");
					}
				}
				writer.writeLine();
			}
			
			
		} catch (DataTableOperationException e) {
			log.error(e);
		} catch (IOException e) {
			log.error("Could not write to file. {}", e);
		} finally {
			try {
				if(main != null)
					main.close();
			} catch (DataTableOperationException e) {
				log.error("Could not close '{}'. {}", main.getName(), e);
			}
			
			try {
				if(additionalColumns != null)
					additionalColumns.close();
			} catch	(DataTableOperationException e) {
				log.error("Could not close '{}'. {}", additionalColumns.getName(), e);
			}
			try {
				if(writer != null)
					writer.close();
			} catch (IOException e) {
				log.error("Could not close writer for table '{}'", main.getName());
			}
		}
		
		return null;
	}

	/**
	 * Finds the index of the keyColumnName in a given header
	 * @param header
	 * @return -1 if keyColumn was not found
	 */
	private int findKeyColumnInTable(List<RowElement> header) {
		for(int i = 0; i < header.size(); i++) {
			if(header.get(i).getContent().equalsIgnoreCase(keyColumnName))
				return i;
		}
		return -1;
	}

	/**
	 * Merges headers of main table and of additional columns
	 * @param mainHeader
	 * @param additionalHeader
	 * @return extended header excluding the key column of the extra table
	 */
	private List<RowElement> createExtendedHeader(List<RowElement> mainHeader, List<RowElement> additionalHeader) {
		List<RowElement> extendedHeader = new ArrayList<>(mainHeader.size() + additionalHeader.size() - 1);
		mainHeader.stream().forEachOrdered(e -> extendedHeader.add(e));
		IntStream
			.range(1, additionalHeader.size())
			.forEach(i -> extendedHeader.add(additionalHeader.get(i)));

		return extendedHeader;
	}

	/**
	 * Creates a map of the mapping key onto the column values. Values with empty keys are ignored.
	 * @param additionalColumns
	 * @return
	 * @throws DataTableOperationException
	 */
	private Map<String, List<RowElement>> createContentMap(DataTable additionalColumns) throws DataTableOperationException {
		Map<String,List<RowElement>> contentMap = new HashMap<>();
		
		//remove header
		if(additionalColumns.hasMoreRows())
			additionalColumns.getNextRow(true);
		StringBuilder sb = new StringBuilder();
		while(additionalColumns.hasMoreRows()) {
			List<RowElement> row = additionalColumns.getNextRow(true);
			String key = row.get(0).getContent();
			
			if(key == null || key.isEmpty()) {
				IntStream
					.range(1, row.size())
					.forEachOrdered(i -> {
						sb.append(row.get(i).getContent());
						sb.append(additionalColumns.getDelimiter());
					});
				sb.deleteCharAt(sb.length() - 1);
				log.warn("Ignoring empty key for values: [{}]", sb.toString());
				sb.setLength(0);
				continue;
			}
			
			contentMap.put(key, row.subList(1, row.size()));
		}
		
		return contentMap;
	}

}
