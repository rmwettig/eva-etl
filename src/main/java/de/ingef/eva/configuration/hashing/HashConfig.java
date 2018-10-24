package de.ingef.eva.configuration.hashing;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.LocalDate;
import java.util.*;
import java.util.function.BiFunction;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import de.ingef.eva.configuration.Configuration;
import de.ingef.eva.services.ConnectionFactory;
import de.ingef.eva.services.TaskRunner;
import de.ingef.eva.utility.DateFormatValidator;
import de.ingef.eva.utility.io.CsvReader;
import de.ingef.eva.utility.io.CsvWriter;
import org.apache.commons.codec.digest.DigestUtils;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.extern.log4j.Log4j2;

import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;

@Getter
@Setter
@NoArgsConstructor
@Log4j2
public class HashConfig {

	private static final String NO_ICD_VALUE = "0000";
	private static final String NO_PZN_VALUE = "00000000";
	private static final String PID_INDEX_NAME = "pidIndex";
	//field names for vers_stamm table
	private static final String DOB_INDEX_NAME = "dobIndex";
	private static final String DOD_INDEX_NAME = "dodIndex";
	private static final String GENDER_INDEX_NAME = "genderIndex";
	//field names for vers_region table
	private static final String KGS_INDEX_NAME = "kgsIndex";
	//field names for arzt_diagnose
	private static final String ICD_INDEX_NAME = "icdIndex";
	private static final String CONFIDENCE_INDEX_NAME = "confidenceIndex";
	private static final String CONTRACT_INDEX_NAME = "contractIndex";
	private static final String DIAGNOSIS_CONFIDENCE = "G";
	private static final String CONTRACT_TYPE = "kv";
	//field names for am_evo
	private static final String PZN_INDEX_NAME = "pznIndex";
	private static final String TMP_FILE_EXTENSION = "tmp";
	private static final String ORIGINAL_FILE_EXTENSION = "orig";
	private static final String MATCH_DOT = "\\.";
	private static final String PID_HASH_COLUMN_NAME = "pid_hash";

	private Path hashFile;
	private int minYear;
	private int maxYear;
	/**
	 * file name to column name to column index map
	 */
	private Map<String, Map<String, Integer>> fileDescriptors;

	@Getter
	@RequiredArgsConstructor
	private static class HashDataPaths {
		private final String dataset;
		private final Path rootPath;
		private final List<Path> dataFiles = new ArrayList<>();

		/**
		 * sorts the collected paths inline using the following rules:
		 * 	* first, sort paths by year, i.e. slices for 2010 preceed those for 2011 and so on
		 * 	* second, apply category order: base data -> kgs -> icd -> pzn
		 */
		public List<Path> sortPathsByDataSliceAndYear() {
			dataFiles.sort((pathA, pathB) -> {
				//name schema is: abc.yyyy.csv.gz
				String[] fileNamePartsA = pathA.toString().split(MATCH_DOT);
				String[] fileNamePartsB = pathB.toString().split(MATCH_DOT);
				int yearA = Integer.parseInt(fileNamePartsA[1]);
				int yearB = Integer.parseInt(fileNamePartsB[1]);
				String nameA = fileNamePartsA[0];
				String nameB = fileNamePartsB[0];
				if(yearA == yearB) {
					if(nameA.equalsIgnoreCase(nameB))
						return 0;
					if(nameA.toLowerCase().contains("vers_stamm"))
						return -1;
					if(nameB.toLowerCase().contains("vers_stamm"))
						return 1;

					if(nameA.toLowerCase().contains("vers_region") && !nameB.contains("vers_stamm"))
						return -1;
					if(nameB.toLowerCase().contains("vers_region") && !nameA.contains("vers_stamm"))
						return 1;

					if(nameA.toLowerCase().contains("arzt_diagnose") && !nameB.contains("am_evo"))
						return -1;
					if(nameB.toLowerCase().contains("arzt_diagnose") && !(nameA.contains("vers_region") && nameA.contains("vers_stamm")))
						return 1;

					return 1;
				} else if(yearA < yearB) {
					return -1;
				} else {
					return 1;
				}
			});
			return dataFiles;
		}
	}

	@Getter
	@RequiredArgsConstructor
	private static class HashDataFiles implements FileVisitor<Path> {

		private Map<String, HashDataPaths> hashFiles = new HashMap<>();
		private final int minYear;
		private final int maxYear;

		@Override
		public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
			return FileVisitResult.CONTINUE;
		}

		@Override
		public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
			return FileVisitResult.CONTINUE;
		}

		@Override
		public FileVisitResult visitFileFailed(Path file, IOException exc) throws IOException {
			return FileVisitResult.CONTINUE;
		}

		@Override
		public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
			boolean isLeaf = Files.list(dir).filter(Files::isDirectory).count() == 0;
			if(!isLeaf)
				return FileVisitResult.CONTINUE;
			HashDataPaths entry = new HashDataPaths(dir.getFileName().toString(), dir.subpath(0, dir.getNameCount() - 1));
			for(File file : dir.toFile().listFiles()) {
				String fileName = file.getName().toLowerCase();
				if(isInvalidFile(fileName))
					continue;

				String[] nameParts = fileName.split(MATCH_DOT);
				if(nameParts.length < 2 || nameParts[1].isEmpty())
					continue;

				int yearSlice = Integer.parseInt(nameParts[1]);
				//calculation is based on specified year limits
				if(yearSlice >= minYear && yearSlice <= maxYear)
					entry.getDataFiles().add(file.toPath());
			}
			hashFiles.put(dir.getFileName().toString(), entry);
			return FileVisitResult.CONTINUE;
		}

		/**
		 * excludes temporary, back-up and for the hash irrelevant files
		 * @param fileName
		 * @return true if file must be excluded
		 */
		private boolean isInvalidFile(String fileName) {
			return fileName.endsWith(TMP_FILE_EXTENSION) ||
			fileName.endsWith(ORIGINAL_FILE_EXTENSION) ||
			!(fileName.contains("vers_stamm") ||
				fileName.contains("am_evo") ||
				fileName.contains("arzt_diagnose") ||
				fileName.contains("vers_region")
			);
		}
	}

	@Getter
	@RequiredArgsConstructor
	@EqualsAndHashCode(of={"pid"})
	private static class DataEntry {
		private final String pid;
		private char gender = '9';
		private LocalDate dob = LocalDate.MAX;
		private LocalDate dod = LocalDate.MAX;
		private String minKgs = "999999";
		private String maxKgs = "000000";
		private final List<String> icdCodes = new ArrayList<>(300);
		private final List<String> pznCodes = new ArrayList<>(20);
		
		public void updateGender(char value) {
			if(value < gender)
				gender = value;
		}
		
		public void updateDOB(LocalDate value) {
			dob = takeMinimumDate(dob, value);
		}
		
		public void updateDOD(LocalDate value) {
			dod = takeMinimumDate(dod, value);
		}
		
		public void addICDCode(String code) {
			icdCodes.add(code);
		}
		
		public void updateMinKgs(String value) {
			minKgs = value.compareTo(minKgs) < 0 ? value : minKgs;
		}
		
		public void updateMaxKgs(String value) {
			maxKgs = value.compareTo(maxKgs) > 0 ? value : maxKgs;
		}
		
		public void addPZN(String pzn) {
			pznCodes.add(pzn);
		}
		
		private LocalDate takeMinimumDate(LocalDate left, LocalDate right) {
			return left.isBefore(right) ? left : right;
		}

		/**
		 * @return true if no field has been changed
		 */
		private boolean isDefault() {
			return gender == '9'
					&& dob.isEqual(LocalDate.MAX)
					&& dod.isEqual(LocalDate.MAX)
					&& minKgs.equals("999999")
					&& maxKgs.equals("000000")
					&& icdCodes.isEmpty()
					&& pznCodes.isEmpty();
		}

		public char getGenderOrDefault() {
			char g = getGender();
			if(g == '9')
				return '0';
			return g;
		}

		public LocalDate getDobOrDefault() {
			return dob.isEqual(LocalDate.MAX)
					? LocalDate.parse("1900-01-01")
					: dob;
		}

		public LocalDate getDodOrDefault() {
			return dod.isEqual(LocalDate.MAX)
					? LocalDate.parse("2999-12-31")
					: dod;
		}

		public String getMinKgsOrDefault() {
			return minKgs.equals("999999")
					? "00000"
					: minKgs;
		}

		public String getMaxKgsOrDefault() {
			return maxKgs.equals("000000")
					? "00000"
					: maxKgs;
		}

		public List<String> getICDOrDefault() {
			return icdCodes.isEmpty()
					? Arrays.asList(NO_ICD_VALUE)
					: icdCodes;
		}

		public List<String> getPZNOrDefault() {
			return pznCodes.isEmpty()
					? Arrays.asList(NO_PZN_VALUE)
					: pznCodes;
		}
	}

	public void calculateHashes(Configuration config, TaskRunner taskRunner, ConnectionFactory connectionFactory) {
		try {
			Map<String, HashDataPaths> hashFiles = findHashData(config.getCacheDirectory());
			for(String dataset : hashFiles.keySet()) {
				HashDataPaths entry = hashFiles.get(dataset);
				Map<String, DataEntry> data = readHashData(entry.sortPathsByDataSliceAndYear());
				appendHash(entry, data);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private void appendHash(HashDataPaths entry, Map<String, DataEntry> data) throws IOException {
		List<Path> modifiableFiles =
				entry
					.getDataFiles()
					.stream()
					.filter(path -> path.getFileName().toString().toLowerCase().contains("vers_stamm"))
					.collect(Collectors.toList());
		for(Path path : modifiableFiles) {
			BufferedReader reader = CsvReader.createGzipReader(path).getReader();
			Path tmpFile = Paths.get(path.toString() + "." + TMP_FILE_EXTENSION);
			CsvWriter writer = CsvWriter.createGzipWriter(tmpFile);
			//check if hash column is already present
			String[] columns = reader.readLine().split(";");
			Arrays.stream(columns).forEach(columnName -> writer.addEntry(columnName));
			int hashColumnIndex = findColumnIndex(columns, PID_HASH_COLUMN_NAME);
			if(hashColumnIndex == -1)
				writer.addEntry(PID_HASH_COLUMN_NAME);
			writer.writeLine();
			int pidColumnIndex = findColumnIndex(columns, "pid");
			Function<String[], String[]> lineProcessor = createLineProcessor(data, hashColumnIndex, pidColumnIndex);
			String line;
			while((line = reader.readLine()) != null) {
				String[] modifiedLine = lineProcessor.apply(line.split(";", -1));
				Arrays.stream(modifiedLine).forEach(value -> writer.addEntry(value));
				writer.writeLine();
			}

			reader.close();
			writer.close();
			swapFiles(path, tmpFile);
		}
	}

	private void swapFiles(Path path, Path tmpFile) throws IOException {
		Files.move(path, Paths.get(path.toString() + "." + ORIGINAL_FILE_EXTENSION), REPLACE_EXISTING);
		Files.move(tmpFile, path);
	}

	private Function<String[], String[]> createLineProcessor(Map<String, DataEntry> mapping, int hashColumnIndex, int pidColumnIndex) {
		return hashColumnIndex != -1
			//update hash
			? columns -> {
				String pid = columns[pidColumnIndex];
				columns[hashColumnIndex] = mapping.containsKey(pid)
						? createHashMapping(mapping.get(pid))
						: "";
				return columns;
			}
			//create new column
			: columns -> {
				String[] modified = new String[columns.length + 1];
				for(int i = 0; i < columns.length; i++) {
					modified[i] = columns[i];
				}
				modified[columns.length] = mapping.containsKey(columns[pidColumnIndex])
						? createHashMapping(mapping.get(columns[pidColumnIndex]))
						: "";
				return modified;
			};
	}

	/**
	 * searches pid_hash column
	 * @param line
	 * @param columnName name of the searched column
	 * @return -1 if pid_hash column does not exist
	 */
	private int findColumnIndex(String[] line, String columnName) {
		for(int i = 0; i < line.length; i++) {
			if(line[i].equalsIgnoreCase(columnName))
				return i;
		}
		return -1;
	}

	private Map<String, DataEntry> readHashData(List<Path> files) throws IOException {
		int numberOfYearBlocks = files.size() / 4;
		Map<String, DataEntry> collectedDataOverYears = new HashMap<>(300_000);
		for(int blockIndex = 0; blockIndex < numberOfYearBlocks; blockIndex++) {
			int offset = 4 * blockIndex;
			Path baseFilePath = files.get(0 + offset);
			String baseCommonName = baseFilePath.getFileName().toString().split(MATCH_DOT)[0];
			Map<String, DataEntry> baseData = readData(
					baseFilePath,
					createBaseDataEntry(baseCommonName),
					this::mergeBaseDataEntries
			);
			Path kgsFilePath = files.get(1 + offset);
			String kgsCommonName = kgsFilePath.getFileName().toString().split(MATCH_DOT)[0];
			Map<String, DataEntry> kgsData = readData(
					kgsFilePath,
					createKgsDataEntry(kgsCommonName),
					this::mergeKgsEntries
			);
			Path icdFilePath = files.get(2 + offset);
			String icdCommonName = icdFilePath.getFileName().toString().split(MATCH_DOT)[0];
			Map<String, DataEntry> icdData = readData(
					icdFilePath,
					createIcdEntry(icdCommonName),
					this::mergeIcdEntries,
					filterIcdEntries(icdCommonName)
			);
			Path pznFilePath = files.get(3 + offset);
			String pznCommonName = pznFilePath.getFileName().toString().split(MATCH_DOT)[0];
			Map<String, DataEntry> pznData = readData(
					pznFilePath,
					createPznEntry(pznCommonName),
					this::mergePznEntry
			);

			aggregateData(collectedDataOverYears, baseData, this::mergeBaseDataEntries);
			aggregateData(collectedDataOverYears, kgsData, this::mergeKgsEntries);
			aggregateData(collectedDataOverYears, icdData, this::mergeIcdEntries);
			aggregateData(collectedDataOverYears, pznData, this::mergePznEntry);
		}
		return collectedDataOverYears;
	}

	/**
	 * merges data into aggregator map
	 * @param aggregator modifiable map
	 * @param data data slice
	 * @param merger merges entries for same keys properly
	 */
	private void aggregateData(Map<String, DataEntry> aggregator, Map<String, DataEntry> data, BiFunction<DataEntry, DataEntry, DataEntry> merger) {
		data
			.entrySet()
			.stream()
			.forEach(entry -> aggregator.merge(entry.getKey(), entry.getValue(), merger));
	}

	private DataEntry mergePznEntry(DataEntry e1, DataEntry e2) {
		e1.getPznCodes().addAll(e2.getPznCodes());
		return e1;
	}

	private Function<List<String>, DataEntry> createPznEntry(String pznCommonName) {
		return columns -> {
			Map<String, Integer> columnIndices = fileDescriptors.get(pznCommonName);
			int pidIndex = columnIndices.get(PID_INDEX_NAME);
			int pznIndex = columnIndices.get(PZN_INDEX_NAME);
			String pzn = columns.get(pznIndex);
			DataEntry e = new DataEntry(columns.get(pidIndex));
			e.addPZN(pzn);
			return e;
		};
	}

	private Predicate<List<String>> filterIcdEntries(String icdCommonName) {
		return columns -> {
			Map<String, Integer> columnIndices = fileDescriptors.get(icdCommonName);
			int confidenceIndex = columnIndices.get(CONFIDENCE_INDEX_NAME);
			int contractIndex = columnIndices.get(CONTRACT_INDEX_NAME);
			return columns.get(confidenceIndex).equalsIgnoreCase(DIAGNOSIS_CONFIDENCE) &&
					columns.get(contractIndex).equalsIgnoreCase(CONTRACT_TYPE);
		};
	}

	private DataEntry mergeIcdEntries(DataEntry e1, DataEntry e2) {
		e1.getIcdCodes().addAll(e2.getIcdCodes());
		return e1;
	}

	private Function<List<String>, DataEntry> createIcdEntry(String icdCommonName) {
		return columns -> {
			Map<String, Integer> columnIndices = fileDescriptors.get(icdCommonName);
			int pidIndex = columnIndices.get(PID_INDEX_NAME);
			int icdIndex = columnIndices.get(ICD_INDEX_NAME);

			String icd = columns.get(icdIndex);
			DataEntry e = new DataEntry(columns.get(pidIndex));
			e.addICDCode(icd);
			return e;
		};
	}

	private DataEntry mergeKgsEntries(DataEntry e1, DataEntry e2) {
		e1.updateMinKgs(e2.getMinKgs());
		e1.updateMaxKgs(e2.getMaxKgs());
		return e1;
	}

	private Function<List<String>, DataEntry> createKgsDataEntry(String kgsCommonName) {
		return columns -> {
			Map<String, Integer> columnIndices = fileDescriptors.get(kgsCommonName);
			int pidIndex = columnIndices.get(PID_INDEX_NAME);
			int kgsIndex = columnIndices.get(KGS_INDEX_NAME);
			String kgs = columns.get(kgsIndex);
			DataEntry e = new DataEntry(columns.get(pidIndex));
			e.updateMinKgs(kgs);
			e.updateMaxKgs(kgs);
			return e;
		};
	}

	private DataEntry mergeBaseDataEntries(DataEntry e1, DataEntry e2) {
		e1.updateDOD(e2.getDod());
		e1.updateDOB(e2.getDob());
		e1.updateGender(e2.getGender());
		return e1;
	}

	private Function<List<String>, DataEntry> createBaseDataEntry(String baseCommonName) {
		return columns -> {
			Map<String, Integer> columnIndices = fileDescriptors.get(baseCommonName);
			int pidIndex = columnIndices.get(PID_INDEX_NAME);
			int dobIndex = columnIndices.get(DOB_INDEX_NAME);
			int dodIndex = columnIndices.get(DOD_INDEX_NAME);
			int genderIndex = columnIndices.get(GENDER_INDEX_NAME);
			DataEntry e = new DataEntry(columns.get(pidIndex));
			String gender = columns.get(genderIndex);
			if(!gender.isEmpty())
				e.updateGender(gender.charAt(0));
			// do not update dates if not ISO date
			DateFormatValidator isoDateFormat = new DateFormatValidator("yyyy-MM-dd");
			String dodText = columns.get(dodIndex);
			if (!dodText.isEmpty() && isoDateFormat.isValid(dodText))
				e.updateDOD(LocalDate.parse(dodText));
			String dobText = columns.get(dobIndex);
			if (!dobText.isEmpty() && isoDateFormat.isValid(dobText))
				e.updateDOB(LocalDate.parse(dobText));
			return e;
		};
	}

	/**
	 * reads data from stream without filtering stream elements
	 * @param baseFilePath
	 * @param rowMapper
	 * @param mapMerger
	 * @return
	 * @throws IOException
	 */
	private Map<String, DataEntry> readData(
			Path baseFilePath,
			Function<List<String>, DataEntry> rowMapper,
			BinaryOperator<DataEntry> mapMerger
	) throws IOException {
		return readData(baseFilePath, rowMapper, mapMerger, columns -> true);
	}

	private Map<String, DataEntry> readData(
			Path baseFilePath,
			Function<List<String>, DataEntry> rowMapper,
			BinaryOperator<DataEntry> mapMerger,
			Predicate<List<String>>	rowFilter
	) throws IOException {
		CsvReader baseReader = CsvReader.createGzipReader(baseFilePath);
		Map<String, DataEntry> baseData =
				baseReader
						.lines()
						.skip(1)
						.filter(rowFilter)
						.map(rowMapper)
						.collect(Collectors.toMap(entry -> entry.getPid(), entry -> entry, mapMerger));
		baseReader.close();
		return baseData;
	}

	private Map<String, HashDataPaths> findHashData(String cacheDirectory) throws IOException {
		HashDataFiles fileFinder = new HashDataFiles(minYear, maxYear);
		Files.walkFileTree(Paths.get(cacheDirectory), fileFinder);
		return fileFinder.getHashFiles();
	}

	/**
	 * combines all data parts in a single data entry object
	 * @param dataEntries
	 * @return
	 */
	private DataEntry combineCategoriesIntoSingleEntry(List<DataEntry> dataEntries) {
		DataEntry base = dataEntries.get(0);
		DataEntry combined = new DataEntry(base.getPid());
		combined.updateGender(base.getGender());
		combined.updateDOB(base.getDob());
		combined.updateDOD(base.getDod());

		DataEntry residence = dataEntries.get(1);
		combined.updateMinKgs(residence.getMinKgs());
		combined.updateMaxKgs(residence.getMaxKgs());

		DataEntry icd = dataEntries.get(2);
		combined.getIcdCodes().addAll(icd.getIcdCodes());

		DataEntry pzn = dataEntries.get(3);
		combined.getPznCodes().addAll(pzn.getPznCodes());
		return combined;
	}

	private String createHashMapping(DataEntry pidData) {
		return DigestUtils.sha256Hex(buildFullDataString(pidData));
	}

	private String buildFullDataString(DataEntry data) {
		StringBuilder dataString = new StringBuilder();
		dataString.append(data.getDobOrDefault());
		dataString.append("_");
		dataString.append(data.getDodOrDefault());
		dataString.append("_");
		dataString.append(data.getGenderOrDefault());
		dataString.append("_");
		dataString.append(data.getMinKgsOrDefault());
		dataString.append("_");
		dataString.append(data.getMaxKgsOrDefault());
		dataString.append("_");
		dataString.append(data.getICDOrDefault().stream().collect(Collectors.joining("_")));
		dataString.append("_");
		dataString.append(data.getPZNOrDefault().stream().collect(Collectors.joining("_")));
		return dataString.toString();
	}
}
