package de.ingef.eva.etl;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import de.ingef.eva.configuration.append.AppendColumnConfig;
import de.ingef.eva.configuration.append.AppendSourceConfig;
import de.ingef.eva.data.RowElement;
import de.ingef.eva.data.SimpleRowElement;
import de.ingef.eva.data.TeradataColumnType;
import de.ingef.eva.utility.Helper;
import lombok.RequiredArgsConstructor;

/**
 * Appends columns loaded from a file to the end of the row
 * @author Martin.Wettig
 *
 */
@RequiredArgsConstructor
public class DDDTransformer extends Transformer {

	private final String db;
	private final String table;
	private final String keyColumn;
	private final Map<String,Map<String,RowElement>> pzn2Column2Value;
	private final List<RowElement> additionalColumnNames;
	private final Map<String,DateRange> pzn2ValidityDates;
	private static final String PACKAGE_COUNT_COLUMN_NAME = "Anzahl_Packungen";
	private static final String PRESCRIPTION_DATE_COLUMN_NAME = "Verordnungsdatum";
	private static final String IS_CLASSIFIED_COLUMN_NAME = "IS_CLASSIFIED";
	private static final DateTimeFormatter WIDO_DATE_FORMAT = DateTimeFormatter.ofPattern("yyyyMMdd");
	private static final int EXPECTED_PZN_LENGTH = 8;
	
	@Override
	public Row transform(Row row) {
		if(canProcessRow(row.getDb(), row.getTable()))
			return process(row);
		return row;
	}

	private boolean canProcessRow(String rowDb, String rowTable) {
		boolean isTablePresent = table != null && !table.isEmpty();
		boolean isDbPresent = db != null && !db.isEmpty();
		
		if(isTablePresent && isDbPresent)
			return db.equalsIgnoreCase(rowDb) && table.equalsIgnoreCase(rowTable);
		if(isTablePresent && !isDbPresent)
			return table.toLowerCase().contains(rowTable.toLowerCase());
		if(!isTablePresent && isDbPresent)
			return db.equalsIgnoreCase(rowDb);
		
		return false; 
	}
	
	private Row process(Row row) {
		if(!row.getColumnName2Index().containsKey(keyColumn))
			return row;
		int keyIndex = row.getColumnName2Index().get(keyColumn);
		String keyValue = row.getColumns().get(keyIndex).getContent();
		List<RowElement> columns = transformColumns(row.getColumns(), keyValue);
		Map<String, Integer> columnIndices = transformColumnIndices(row.getColumnName2Index());
		calculateDDD(columns, columnIndices, pzn2ValidityDates.get(keyValue));
		return new Row(row.getDb(), row.getTable(), columns, columnIndices);
	}

	private void calculateDDD(List<RowElement> columns, Map<String, Integer> columnIndices, DateRange validityDates) {
		if(!columnIndices.containsKey(PACKAGE_COUNT_COLUMN_NAME)) return;
		double packageCount = Double.parseDouble(columns.get(columnIndices.get(PACKAGE_COUNT_COLUMN_NAME)).getContent());
		int dddIndex = columnIndices.get(WidoColumn.DDDPK.getLabel());
		RowElement dddElement = columns.get(dddIndex);
		String numberString = dddElement.getContent();
		if(!numberString.isEmpty()) {
			int ddd = Integer.parseInt(numberString);
			double result = ddd/1000.0 * packageCount;
			result = calculateCheckedDDD(result, columns, columnIndices, validityDates);
			columns.set(dddIndex, new SimpleRowElement(Double.toString(result), dddElement.getType()));
		}
	}

	private double calculateCheckedDDD(double preliminaryDDD, List<RowElement> columns, Map<String, Integer> columnIndices, DateRange validityDates) {
		//pzn without date entry are always valid
		if(validityDates == null) return preliminaryDDD;
		
		int prescriptionDateIndex = columnIndices.get(PRESCRIPTION_DATE_COLUMN_NAME);
		LocalDate prescriptionDate = LocalDate.parse(columns.get(prescriptionDateIndex).getContent());
		boolean isValidAdmissionDate = !validityDates.getStart().isEqual(LocalDate.MIN);
		boolean isValidRetirementDate = !validityDates.getEnd().isEqual(LocalDate.MAX);
		//drug was admitted and not retired
		if(isValidAdmissionDate && !isValidRetirementDate) {
			//prescription happened on or after admission
			return prescriptionDate.isAfter(validityDates.getStart()) || prescriptionDate.isEqual(validityDates.getStart()) ? preliminaryDDD : 0;
		}
		//drug was retired but admission is unknown
		if(isValidRetirementDate && !isValidAdmissionDate) {
			return prescriptionDate.isBefore(validityDates.getEnd()) ? preliminaryDDD : 0;
		}
		//drug was admitted and retired already
		if(isValidAdmissionDate && isValidRetirementDate) {
			return (prescriptionDate.isAfter(validityDates.getStart()) || prescriptionDate.isEqual(validityDates.getStart())) && prescriptionDate.isBefore(validityDates.getEnd()) ? preliminaryDDD : 0;
		}
		
		return 0.0;
	}

	private Map<String, Integer> transformColumnIndices(Map<String, Integer> columnName2Index) {
		Map<String, Integer> transformedIndices = new HashMap<>(columnName2Index.size() + additionalColumnNames.size());
		columnName2Index.forEach((column,index) -> transformedIndices.put(column, index));
		IntStream
			.range(0, additionalColumnNames.size())
			//first empty position is at columnName2Index.size() and is pushed forwards by adding the index
			.forEachOrdered(i -> transformedIndices.put(additionalColumnNames.get(i).getContent(), i + columnName2Index.size()));
		return transformedIndices;
	}

	private List<RowElement> transformColumns(List<RowElement> columns, String keyValue) {
		List<RowElement> transformedColumns = new ArrayList<>(columns.size() + pzn2Column2Value.size());
		transformedColumns.addAll(columns);
		//if there is no data for the key add empty elements to pad row size
		if(!pzn2Column2Value.containsKey(keyValue)) {
			IntStream
				.range(0, additionalColumnNames.size())
				.forEach(i -> transformedColumns.add(new SimpleRowElement("", TeradataColumnType.CHARACTER)));
		} else {
			Map<String,RowElement> column2Value = pzn2Column2Value.get(keyValue); 
			additionalColumnNames
				.stream()
				.map(headerElement -> headerElement.getContent())
				.forEachOrdered(columnName -> {
					transformedColumns.add(takeValueOrCreateEmpty(columnName, column2Value));
				});
		}
		return transformedColumns;
	}
	
	private RowElement takeValueOrCreateEmpty(String columnName, Map<String,RowElement> values) {
		if(!values.containsKey(columnName))
			return new SimpleRowElement("", TeradataColumnType.CHARACTER);
		return values.get(columnName);
	}
	
	public static Transformer of(String targetDb, String targetTable, String keyColumn, List<AppendSourceConfig> sources) throws IOException {		
		//main data is at position 0, supplementary data is at position 1
		List<Map<String, Map<String, RowElement>>> additionalColumns = new ArrayList<>(sources.size());
		HashSet<String> orderedColumnNames = new LinkedHashSet<>();
		HashSet<String> excludeDatesFromResult = createColumnExclusion();
		for(AppendSourceConfig source : sources) {
			BufferedReader reader = Files.newBufferedReader(source.getFile());
			additionalColumns.add(mapKeyOntoData(source.getColumns(), reader, source.getKeyColumnIndex()));
			reader.close();
			source.getColumns()
				.stream()
				.map(column -> column.getColumn().getLabel())
				.filter(name -> !excludeDatesFromResult.contains(name) && !orderedColumnNames.contains(name))
				.forEachOrdered(newName -> orderedColumnNames.add(newName));
		}
		Map<String, Map<String, RowElement>> pzn2Column2Value = consolidateData(additionalColumns);
		Set<String> supplementOnlyKeys = createSupplementKeys(additionalColumns);
		Map<String, DateRange> pzn2ValidityDates = extractValidityDateRange(pzn2Column2Value, supplementOnlyKeys);
		List<RowElement> columnNames = createHeader(orderedColumnNames);
		if(columnNames.stream().anyMatch(e -> e.getContent().equalsIgnoreCase(WidoColumn.STANAME.getLabel()))) {
			columnNames.add(new SimpleRowElement(IS_CLASSIFIED_COLUMN_NAME, TeradataColumnType.CHARACTER));
			pzn2Column2Value = processPackageName(pzn2Column2Value);
		}
		return new DDDTransformer(targetDb, targetTable, keyColumn, pzn2Column2Value, columnNames, pzn2ValidityDates);
	}

	private static Set<String> createSupplementKeys(List<Map<String, Map<String, RowElement>>> additionalColumns) {
		return additionalColumns.get(1)
				.keySet()
				.parallelStream()
				.filter(pzn -> !additionalColumns.get(0).containsKey(pzn))
				.collect(Collectors.toSet());
	}

	private static Map<String, Map<String ,RowElement>> processPackageName(Map<String, Map<String, RowElement>> pzn2Column2Value2) {
		Map<String, Map<String, RowElement>> processedPackageName = new HashMap<>(pzn2Column2Value2.size());
		for(Map.Entry<String, Map<String, RowElement>> entry : pzn2Column2Value2.entrySet()) {
			Map<String, RowElement> column2Value = entry.getValue();
			RowElement nameElement = column2Value.get(WidoColumn.STANAME.getLabel());
			String cleanName = null;
			String classifiedValue = null;
			TeradataColumnType type = TeradataColumnType.CHARACTER;
			//use empty strings if no name column is present
			if(nameElement != null) {
				cleanName = nameElement.getContent().split("\\*NV\\*")[0].trim();
				classifiedValue = nameElement.getContent().contains("NV") ? "0" : "1";
				type = nameElement.getType();
			} else {
				cleanName = "";
				classifiedValue = "";
			}
			column2Value.put(WidoColumn.STANAME.getLabel(), new SimpleRowElement(cleanName, type)); 
			column2Value.put(IS_CLASSIFIED_COLUMN_NAME, new SimpleRowElement(classifiedValue, TeradataColumnType.CHARACTER));
			processedPackageName.put(entry.getKey(), column2Value);
		}
		
		return processedPackageName;
	}

	private static HashSet<String> createColumnExclusion() {
		HashSet<String> excludeDatesFromResult = new HashSet<>(2);
		excludeDatesFromResult.add(WidoColumn.ADMISSION_DATE.getLabel());
		excludeDatesFromResult.add(WidoColumn.RETIREMENT_DATE.getLabel());
		return excludeDatesFromResult;
	}
		
	private static Map<String, DateRange> extractValidityDateRange(Map<String, Map<String, RowElement>> pzn2Column2Value, Set<String> supplementOnlyKeys) {
		Map<String, DateRange> checkedData = new HashMap<>(pzn2Column2Value.size());
		for(Map.Entry<String, Map<String, RowElement>> entry : pzn2Column2Value.entrySet()) {
			
			Map<String,RowElement> column2Value = entry.getValue();
			//if current key came from supplement and was not in main data do not add a date range
			if(supplementOnlyKeys.contains(entry.getKey())) continue;
			boolean hasAdmissionDate = column2Value.containsKey(WidoColumn.ADMISSION_DATE.getLabel());
			boolean hasRetirementDate = column2Value.containsKey(WidoColumn.RETIREMENT_DATE.getLabel());

			LocalDate start = LocalDate.MIN;
			if(hasAdmissionDate) {
				String dateValue = column2Value.get(WidoColumn.ADMISSION_DATE.getLabel()).getContent();
				//set ddd to zero if it is present
				if(!dateValue.isEmpty()) {
					start = LocalDate.parse(dateValue, WIDO_DATE_FORMAT);
				}
				pzn2Column2Value.remove(WidoColumn.ADMISSION_DATE.getLabel());
			}
			
			LocalDate end = LocalDate.MAX;
			if(hasRetirementDate) {
				String dateValue = column2Value.get(WidoColumn.RETIREMENT_DATE.getLabel()).getContent();
				//set ddd to zero if it is present
				if(!dateValue.isEmpty()) {
					end = LocalDate.parse(dateValue, WIDO_DATE_FORMAT);
				}
				pzn2Column2Value.remove(WidoColumn.RETIREMENT_DATE.getLabel());
			}
			checkedData.put(entry.getKey(), new DateRange(start, end));
		}
		return checkedData;
	}

	private static List<RowElement> createHeader(HashSet<String> orderedColumnNames) {
		return orderedColumnNames
				.stream()
				.map(name -> new SimpleRowElement(name, TeradataColumnType.CHARACTER))
				.collect(Collectors.toList());
	}

	/**
	 * updates data for same column with data from later defined sources
	 * @param data
	 * @return column name to element map
	 */
	private static Map<String, Map<String, RowElement>> consolidateData(List<Map<String, Map<String, RowElement>>> data) {
		Map<String, Map<String, RowElement>> mergedData = new HashMap<>();
		data
			.stream()
			.flatMap(map -> map.entrySet().stream())
			.forEach(entry -> {
				mergedData.merge(
						entry.getKey(),
						entry.getValue(),
						(oldValue, newValue) -> {
							Map<String,RowElement> merged = new HashMap<>(oldValue);
							merged.putAll(newValue);
							return merged;
						});
			});
		
		return mergedData;
	}

	/**
	 * associates keys with their respective data
	 * @param columnConfig
	 * @param reader
	 * @param keyIndex
	 * @return a map key to column values
	 * @throws IOException
	 */
	private static Map<String, Map<String, RowElement>> mapKeyOntoData(List<AppendColumnConfig> columnConfig, BufferedReader reader, int keyIndex) throws IOException {
		Map<String, Map<String, RowElement>> pzn2Column2Data = new HashMap<>();
		String line = null;
		while((line = reader.readLine()) != null) {
			if(line.isEmpty()) continue;
			String[] values = line.split(";", -1);
			for(AppendColumnConfig config : columnConfig) {
				String pzn = Helper.addPaddingZeros(values[keyIndex], EXPECTED_PZN_LENGTH);
				if(pzn2Column2Data.containsKey(pzn)) {
					Map<String,RowElement> column2Data = pzn2Column2Data.get(pzn);
					RowElement element = new SimpleRowElement(values[config.getIndex()], TeradataColumnType.ANY);
					column2Data.put(config.getColumn().getLabel(), element);
				} else {
					Map<String,RowElement> column2Data = new HashMap<>();
					RowElement element = new SimpleRowElement(values[config.getIndex()], TeradataColumnType.ANY);
					column2Data.put(config.getColumn().getLabel(), element);
					pzn2Column2Data.put(pzn, column2Data);
				}
			}
		}
		return pzn2Column2Data;
	}
		
	private static Map<String, List<RowElement>> readColumns(BufferedReader reader) throws IOException {
		String line = null;
		Map<String,List<RowElement>> additionalColumns = new HashMap<>();
		while((line = reader.readLine()) != null) {
			if(line.isEmpty())
				continue;
			String[] arr = line.split(";");
			if(arr[0] == null || arr[0].isEmpty())
				continue;
			//first field is the key
			List<RowElement> columns = new ArrayList<>(arr.length - 1);
			IntStream.range(1, arr.length).forEachOrdered(i -> columns.add(new SimpleRowElement(arr[i], TeradataColumnType.CHARACTER)));
			additionalColumns.put(arr[0], columns);
		}
		
		return additionalColumns;
	}

	private static List<RowElement> readHeader(BufferedReader reader) throws IOException {
		String header = reader.readLine();
		String[] names = header.split(";");
		List<RowElement> columnNames = new ArrayList<>(names.length);
		for(String name : names)
			columnNames.add(new SimpleRowElement(name, TeradataColumnType.CHARACTER));
		return columnNames;
	}
}
