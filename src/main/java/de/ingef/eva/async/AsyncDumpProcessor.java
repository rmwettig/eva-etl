package de.ingef.eva.async;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Collection;

import org.apache.logging.log4j.Logger;
import de.ingef.eva.processor.Processor;
import de.ingef.eva.utility.Dataset;

/**
 * Represents a asynchronous database dump processing job. It iterates over the
 * file contents, removes control sequences and writes the cleaned row to a
 * single file.
 * 
 * @author Martin Wettig
 *
 */
public class AsyncDumpProcessor implements Runnable {
	private Collection<Processor<String>> _processors;
	private Dataset _data;
	private String _outDirectory;
	private String _outFilename;
	private String _rowStartSignal;
	private Logger _logger;

	public AsyncDumpProcessor(Collection<Processor<String>> processor, Dataset dataset, String outDirectory,
			String outFilename, String rowStartSignal, Logger logger) {
		_processors = processor;
		_data = dataset;
		_outDirectory = outDirectory;
		_outFilename = outFilename;
		_rowStartSignal = rowStartSignal;
		_logger = logger;
	}

	@Override
	public void run() {
		File outputFile = new File(_outDirectory + "/" + _outFilename);
		try (BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile, outputFile.exists()));) {
			//if output file is empty add a header
			if (outputFile.length() == 0)
				writeHeader(writer, _data.getHeaderFile());
			
			for (File file : _data.getData()) {
				if (_logger != null)
					_logger.info("Processing {}", file.getName());
				BufferedReader reader = new BufferedReader(new FileReader(file));
				String line;
				while ((line = reader.readLine()) != null) {
					if (!line.isEmpty()) {
						String processedLine = removeLeadingJunk(line, _rowStartSignal);
						for (Processor<String> processor : _processors)
							processedLine = processor.process(processedLine);
						writer.write(processedLine);
						writer.newLine();
					}
				}
				reader.close();
			}
		} catch (IOException e) {
			if (_logger != null) {
				_logger.error("Could not open file {}.\nStackTrace: {}", _outFilename, e.getStackTrace());
			} else {
				e.printStackTrace();
			}
		}
	}

	private void writeHeader(BufferedWriter writer, File headerFile) throws IOException {
		BufferedReader reader = new BufferedReader(new FileReader(headerFile));
		String header = reader.readLine();
		reader.close();
		writer.write(header);
		writer.newLine();
	}

	private String removeLeadingJunk(String line, String startSignal) {
		int startIndex = line.indexOf(startSignal);
		// start behind start signal
		return line.substring(startIndex + startSignal.length());
	}
}
