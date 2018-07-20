package de.ingef.eva.utility;

import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPOutputStream;

import de.ingef.eva.constant.OutputDirectory;
import lombok.Getter;

public class CsvWriter {
	@Getter
	private String delimiter;
	private String newLine;
	private BufferedWriter writer;
	private Path attachedFile;
	@Getter
	private boolean isNewFile = true;
	private List<String> columnValues = new ArrayList<>(30);
		
	public CsvWriter(BufferedWriter writer, String delimiter, String newLine, Path file) {
		this.delimiter = delimiter;
		this.newLine = newLine;
		this.writer = writer;
		this.attachedFile = file;
	}
			
	public void close() throws IOException {
		writer.close();
	}
	
	/**
	 * adds a column value
	 * @param value
	 */
	public void addEntry(String value) {
		columnValues.add(value);
	}
	
	/**
	 * writes collected values to file. The values are delimited by the specified character.
	 * @throws IOException
	 */
	public void writeLine() throws IOException {
		writer.write(String.join(delimiter, columnValues));
		writer.write(newLine);
		columnValues.clear();
		//since compressed files have header information their size is not zero
		//thus, the isNewFile flag is used to determined whether or not data is already present
		if(isNewFile)
			isNewFile = false;
	}
	
	/**
	 * creates a gzipped csv file writer. The written file uses semicolons to separate values and linux line endings
	 * @param file path to file
	 * @return writer instance
	 * @throws FileNotFoundException
	 * @throws IOException
	 */
	public static CsvWriter createGzipWriter(Path file) throws FileNotFoundException, IOException {
		return createGzipWriter(file, ";", "\n");
	}
	
	public static CsvWriter createGzipWriter(Path file, String delimiter, String newline) throws FileNotFoundException, IOException {
		return new CsvWriter(
				new BufferedWriter(new OutputStreamWriter(new GZIPOutputStream(new FileOutputStream(file.toFile())), OutputDirectory.DATA_CHARSET)),
				delimiter,
				newline,
				file
			);
	}
	
	/**
	 * creates an uncompressed csv file writer. The written file uses semicolons to separate values and linux line endings
	 * @param file path to file
	 * @param appendToFile whether or not data should be appended to the given file
	 * @return writer instance
	 * @throws FileNotFoundException
	 */
	public static CsvWriter createUncompressedWriter(Path file, boolean appendToFile) throws FileNotFoundException {
		return createUncompressedWriter(file, appendToFile, ";", "\n");
	}
	
	public static CsvWriter createUncompressedWriter(Path file, boolean appendToFile, String delimiter, String newline) throws FileNotFoundException {
		return new CsvWriter(
				new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file.toFile(), appendToFile), OutputDirectory.DATA_CHARSET)),
				delimiter,
				newline,
				file
			);
	}
}
