package de.ingef.eva.utility;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import lombok.Getter;

public class CsvWriter {
	@Getter
	private String delimiter = ";";
	private String newLine = "\n";
	private File outputFile;
	private BufferedWriter writer;
	
	private StringBuffer line = new StringBuffer();
	
	public CsvWriter(String delimiter, String newLine) {
		this.delimiter = delimiter;
		this.newLine = newLine;
	}
	
	public CsvWriter(File file) {
		outputFile = file;
	}
	
	public void open() throws IOException {
		writer = new BufferedWriter(new FileWriter(outputFile));
	}
	
	public void close() throws IOException {
		writer.close();
	}
	
	public void addEntry(String value) {
		line.append(value);
		line.append(delimiter);
	}
	
	public void writeLine() throws IOException {
		//remove trailing delimiter
		line.deleteCharAt(line.length() - 1);
		writer.write(line.toString());
		writer.write(newLine);
		line = new StringBuffer();
	}
	
	
}
