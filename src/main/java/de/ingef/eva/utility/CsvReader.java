package de.ingef.eva.utility;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.GZIPInputStream;

import de.ingef.eva.constant.OutputDirectory;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class CsvReader {

	private final BufferedReader reader;
	private final String delimiter;
	
	/**
	 * creats a gzipped csv file reader
	 * @param csvFile path to file
	 * @param delimiter delimiter used to separate columns
	 * @return reader instance
	 * @throws FileNotFoundException
	 * @throws IOException
	 */
	public static CsvReader createGzipReader(Path csvFile, String delimiter) throws FileNotFoundException, IOException {
		return new CsvReader(
			new BufferedReader(new InputStreamReader(new GZIPInputStream(new FileInputStream(csvFile.toFile())), OutputDirectory.DATA_CHARSET)),
			delimiter
		);
	}
	
	public static CsvReader createGzipReader(Path csvFile) throws FileNotFoundException, IOException {
		return createGzipReader(csvFile, ";");
	}
	
	/**
	 * creates a regular csv file reader
	 * @param csvFile path to uncompressed csv file
	 * @param delimiter separation character
	 * @return reader instance
	 * @throws FileNotFoundException
	 * @throws IOException
	 */
	public static CsvReader createReader(Path csvFile, String delimiter) throws FileNotFoundException, IOException {
		return new CsvReader(
			new BufferedReader(new InputStreamReader(new FileInputStream(csvFile.toFile()), OutputDirectory.DATA_CHARSET)),
			delimiter
		);
	}
	
	public static CsvReader createReader(Path csvFile) throws FileNotFoundException, IOException {
		return createReader(csvFile, ";");
	}
	
	public void close() throws IOException {
		reader.close();
	}
	
	public Stream<List<String>> lines() {
		return reader
			.lines()
			.filter(line -> !line.isEmpty())
			.map(line -> Arrays.asList(line.split(delimiter)))
			.collect(Collectors.toList())
			.stream();
	}
	
	
}
