package de.ingef.eva.utility;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPOutputStream;

import de.ingef.eva.constant.OutputDirectory;
import lombok.Getter;

public class CsvWriter {
	@Getter
	private String delimiter;
	private String newLine;
	private File outputFile;
	private GZIPOutputStream writer;
	
	private List<String> columnValues = new ArrayList<>(30);
		
	public CsvWriter(File file, String delimiter, String newLine) {
		this.delimiter = delimiter;
		this.newLine = newLine;
		this.outputFile = file;
	}
	
	public CsvWriter(File file) {
		this(file, ";", "\n");
	}
	
	public void open() throws IOException {
		writer = new GZIPOutputStream(Files.newOutputStream(Paths.get(outputFile.getAbsolutePath())));
	}
	
	public void close() throws IOException {
		writer.flush();
		writer.close();
	}
	
	public void addEntry(String value) {
		columnValues.add(value);
	}
	
	public void writeLine() throws IOException {
		writer.write((String.join(delimiter, columnValues) + newLine).getBytes(OutputDirectory.DATA_CHARSET));
		writer.flush();
		columnValues.clear();
	}
	
	
}
