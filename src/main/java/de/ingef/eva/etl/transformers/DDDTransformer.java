package de.ingef.eva.etl.transformers;

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
import de.ingef.eva.utility.DateRange;
import de.ingef.eva.etl.Row;
import de.ingef.eva.utility.Helper;

/**
 * Appends columns loaded from a wido file to the end of the row
 * @author Martin.Wettig
 *
 */
public class DDDTransformer extends Transformer {

	private final String keyColumn;
	private final Map<String, Map<String, RowElement>> pzn2Column2Value;
	private final List<RowElement> additionalColumnNames;
	private final Map<String, DateRange> pzn2ValidityDates;
	private final Map<String, Map<WidoColumn, RowElement>> pzn2MetaColumns;
	private static final String PACKAGE_COUNT_COLUMN_NAME = "Anzahl_Packungen".toLowerCase();
	private static final String PRESCRIPTION_DATE_COLUMN_NAME = "Verordnungsdatum".toLowerCase();
	private static final String IS_CLASSIFIED_COLUMN_NAME = "IS_CLASSIFIED";
	private static final DateTimeFormatter WIDO_DATE_FORMAT = DateTimeFormatter.ofPattern("yyyyMMdd");
	private static final int EXPECTED_PZN_LENGTH = 8;
	
	public DDDTransformer(
			String db,
			String table,
			String keyColumn,
			Map<String, Map<String, RowElement>> pzn2Column2Value,
			List<RowElement> additionalColumnNames,
			Map<String, DateRange> pzn2ValidityDates,
			Map<String, Map<WidoColumn, RowElement>> pznMetaColumns) {
		super(db, table);
		this.keyColumn = keyColumn;
		this.pzn2Column2Value = pzn2Column2Value;
		this.additionalColumnNames = additionalColumnNames;
		this.pzn2ValidityDates = pzn2ValidityDates;
		this.pzn2MetaColumns = pznMetaColumns;
	}
	
	@Override
	public Row transform(Row row) {
		if(canProcessRow(row.getDb(), row.getTable()))
			return process(row);
		return row;
	}
	
	private Row process(Row row) {
		if(!row.getColumnName2Index().containsKey(keyColumn.toLowerCase()))
			return row;
		int keyIndex = row.getColumnName2Index().get(keyColumn.toLowerCase());
		String keyValue = row.getColumns().get(keyIndex).getContent();
		List<RowElement> columns = transformColumns(row.getColumns(), keyValue);
		Map<String, Integer> columnIndices = transformColumnIndices(row.getColumnName2Index());
		DateRange validityDates = pzn2ValidityDates.get(keyValue);
		deriveCalculatedColumns(columns, columnIndices, validityDates);
		return new Row(row.getDb(), row.getTable(), columns, columnIndices);
	}

	private void deriveCalculatedColumns(List<RowElement> columns, Map<String, Integer> columnIndices, DateRange validityDates) {
		if(columnIndices.containsKey(PACKAGE_COUNT_COLUMN_NAME)) {
			double packageCount = Double.parseDouble(columns.get(columnIndices.get(PACKAGE_COUNT_COLUMN_NAME)).getContent());
			calculateDDD(columns, columnIndices, validityDates, packageCount);
			calculateActualPackageSize(columns, columnIndices, validityDates, packageCount);
		}
	}

	private void calculateDDD(List<RowElement> columns, Map<String, Integer> columnIndices, DateRange validityDates, double packageCount) {
		int dddIndex = columnIndices.get(WidoColumn.DDDPK.getLabel());
		RowElement dddElement = columns.get(dddIndex);
		String numberString = dddElement.getContent();
		if(!numberString.isEmpty()) {
			int ddd = Integer.parseInt(numberString);
			double result = ddd/1000.0 * packageCount;
			result = ensureValidValue(result, columns, columnIndices, validityDates);
			columns.set(dddIndex, new SimpleRowElement(Double.toString(result), dddElement.getType()));
		}
	}
	
	private void calculateActualPackageSize(List<RowElement> columns, Map<String, Integer> columnIndices, DateRange validityDates, double packageCount) {
		String pzn = columns.get(columnIndices.get("pzn")).getContent();
		if(!pzn2MetaColumns.containsKey(pzn) ||
			!pzn2MetaColumns.get(pzn).containsKey(WidoColumn.PACK_SIZE) ||
			pzn2MetaColumns.get(pzn).get(WidoColumn.PACK_SIZE).getContent().isEmpty()) {
			columns.add(new SimpleRowElement("", TeradataColumnType.DECIMAL));
			columnIndices.put("Menge_berechnet", columns.size() - 1);
			return;
		}
		String packSizeString = pzn2MetaColumns.get(pzn).get(WidoColumn.PACK_SIZE).getContent();
		int packSize = Integer.parseInt(packSizeString);
		double result = packSize/10.0 * packageCount;
		result = ensureValidValue(result, columns, columnIndices, validityDates);
		columns.add(new SimpleRowElement(Double.toString(result), TeradataColumnType.DECIMAL));
		columnIndices.put("Menge_berechnet", columns.size() - 1);
	}

	private double ensureValidValue(double preliminaryValue, List<RowElement> columns, Map<String, Integer> columnIndices, DateRange validityDates) {
		//pzn without date entry are always valid
		if(validityDates == null) return preliminaryValue;
		
		int prescriptionDateIndex = columnIndices.get(PRESCRIPTION_DATE_COLUMN_NAME);
		LocalDate prescriptionDate = LocalDate.parse(columns.get(prescriptionDateIndex).getContent());
		boolean isValidAdmissionDate = !validityDates.getStart().isEqual(LocalDate.MIN);
		boolean isValidRetirementDate = !validityDates.getEnd().isEqual(LocalDate.MAX);
		//drug was admitted and not retired
		if(isValidAdmissionDate && !isValidRetirementDate) {
			//prescription happened on or after admission
			return prescriptionDate.isAfter(validityDates.getStart()) || prescriptionDate.isEqual(validityDates.getStart()) ? preliminaryValue : 0;
		}
		//drug was retired but admission is unknown
		if(isValidRetirementDate && !isValidAdmissionDate) {
			return prescriptionDate.isBefore(validityDates.getEnd()) ? preliminaryValue : 0;
		}
		//drug was admitted and retired already
		if(isValidAdmissionDate && isValidRetirementDate) {
			return (prescriptionDate.isAfter(validityDates.getStart()) || prescriptionDate.isEqual(validityDates.getStart())) && prescriptionDate.isBefore(validityDates.getEnd()) ? preliminaryValue : 0;
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
		Set<AppendColumnConfig> metaColumns = new HashSet<>();
		for(AppendSourceConfig source : sources) {
			BufferedReader reader = Files.newBufferedReader(source.getFile());
			additionalColumns.add(mapKeyOntoData(source.getColumns(), reader, source.getKeyColumnIndex()));
			reader.close();
			extractDataColumns(orderedColumnNames, source);
			extractMetaColumns(metaColumns, source);
		}
		Map<String, Map<String, RowElement>> pzn2Column2Value = consolidateData(additionalColumns);
		Set<String> supplementOnlyKeys = createSupplementKeys(additionalColumns);
		Map<String, DateRange> pzn2ValidityDates = extractValidityDateRange(pzn2Column2Value, supplementOnlyKeys);
		List<RowElement> columnNames = createHeader(orderedColumnNames);
		Map<String, Map<WidoColumn, RowElement>> pzn2MetaColumns = createMetaColumnMapping(pzn2Column2Value, metaColumns);
		if(isNameSelected(columnNames)) {
			columnNames.add(new SimpleRowElement(IS_CLASSIFIED_COLUMN_NAME, TeradataColumnType.CHARACTER));
			pzn2Column2Value = processPackageName(pzn2Column2Value);
		}
		return new DDDTransformer(targetDb, targetTable, keyColumn, pzn2Column2Value, columnNames, pzn2ValidityDates, pzn2MetaColumns);
	}

	private static Map<String, Map<WidoColumn, RowElement>> createMetaColumnMapping(Map<String, Map<String, RowElement>> pzn2Column2Value, Set<AppendColumnConfig> metaColumns) {
		Map<String, Map<WidoColumn, RowElement>> metaData = new HashMap<>();
		for(Map.Entry<String, Map<String, RowElement>> entry : pzn2Column2Value.entrySet()) {
			Map<WidoColumn, RowElement> data = new HashMap<>();
			for(AppendColumnConfig config : metaColumns) {
				WidoColumn widoColumn = config.getColumn();
				RowElement metaValue = entry.getValue().getOrDefault(widoColumn.getLabel(), new SimpleRowElement("", TeradataColumnType.ANY));
				data.put(widoColumn, metaValue);
			}
			metaData.put(entry.getKey(), data);
		}
		return metaData;
	}

	private static boolean isNameSelected(List<RowElement> columnNames) {
		return columnNames
				.stream()
				.anyMatch(e -> e.getContent().equalsIgnoreCase(WidoColumn.STANAME.getLabel()));
	}

	private static void extractMetaColumns(Set<AppendColumnConfig> metaColumns, AppendSourceConfig source) {
		source.getColumns()
			.stream()
			.filter(column -> column.isMeta())
			.forEach(metaColumn -> metaColumns.add(metaColumn));
	}

	private static void extractDataColumns(HashSet<String> orderedColumnNames, AppendSourceConfig source) {
		source.getColumns()
			.stream()
			.filter(column -> !column.isMeta())
			.map(column -> column.getColumn().getLabel())
			.filter(name -> !orderedColumnNames.contains(name))
			.forEachOrdered(newName -> orderedColumnNames.add(newName));
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

}
