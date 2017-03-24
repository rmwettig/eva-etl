package de.ingef.eva.async;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;
import java.util.stream.Stream;

import de.ingef.eva.utility.Helper;

public class AsyncMapper implements Runnable {

	private Map<String,String> _mapping;
	private String _fileName;
	private int _mappedColumnIndex;
	private String _newColumnName;
	
	public AsyncMapper(Map<String,String> mapping, String fileName, int mappedColumnIndex, String newColumnName) {
		_mapping = mapping;
		_fileName = fileName;
		_mappedColumnIndex = mappedColumnIndex;
		_newColumnName = newColumnName;
	}
	
	@Override
	public void run() {
		try (
				BufferedReader reader = new BufferedReader(new FileReader(_fileName));
				BufferedWriter writer = new BufferedWriter(new FileWriter(_fileName.replace(".", ".mapped.")));
			) {
			//remove header
			writer.write(reader.readLine() + ";" + _newColumnName);
			writer.newLine();
			
			String line;
			while((line = reader.readLine()) != null) {
				String[] columns = line.split(";");
				for(int i = 0; i < columns.length; i++) {
					writer.write(columns[i]);
					writer.write(";");
				}
				String key = columns[_mappedColumnIndex];
				if (_mapping.containsKey(key))
					writer.write(_mapping.get(key));
				writer.write(";");
				writer.newLine();
			}
			
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
