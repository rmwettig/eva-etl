package de.ingef.eva.utility;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class SimpleFileWriter {
	/**
	 * writes the content to a file
	 * @param path
	 * @param content
	 * @return file object for the output file
	 * @throws IOException
	 */
	public static File writeToFile(String path, String content) throws IOException {
		File outFile = new File(path);
		BufferedWriter writer = new BufferedWriter(new FileWriter(outFile));
		writer.write(content);
		writer.close();
		
		return outFile;
	}
}
