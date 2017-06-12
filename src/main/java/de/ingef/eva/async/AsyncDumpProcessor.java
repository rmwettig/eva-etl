package de.ingef.eva.async;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.logging.log4j.Logger;

import de.ingef.eva.constant.TeradataColumnType;
import de.ingef.eva.database.Column;
import de.ingef.eva.database.Table;
import de.ingef.eva.processor.Processor;
import de.ingef.eva.processor.ReplacePattern;
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
	private Collection<ReplacePattern> _processors;
	private Dataset _data;
	private String _outDirectory;
	private String _outFilename;
	private String _rowStartSignal;
	private Table _table;
	private Logger _logger;

	public AsyncDumpProcessor(
			Collection<ReplacePattern> processor,
			Dataset dataset,
			String outDirectory,
			String outFilename,
			String rowStartSignal,
			Table table,
			Logger logger) {
		_processors = processor;
		_data = dataset;
		_outDirectory = outDirectory;
		_outFilename = outFilename;
		_rowStartSignal = rowStartSignal;
		_logger = logger;
		_table = table;
	}

	@Override
	public void run() {
		File outputFile = new File(_outDirectory + "/" + _outFilename);
		try (BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile));) {

			List<List<Processor<String>>> columnProcessors = null;
			//if output file is empty add a header
			if (outputFile.length() == 0) {
				String header = writeHeader(writer, _data.getHeaderFile());
				columnProcessors = setUpColumnValidation(header);
			}
			
			for (File file : _data.getData()) {
				if (_logger != null)
					_logger.info("Processing {}", file.getName());
				BufferedReader reader = new BufferedReader(new FileReader(file));
				String line;
				while ((line = reader.readLine()) != null) {
					if (!line.isEmpty()) {
						String processedLine = removeLeadingJunk(line, _rowStartSignal);
						StringBuilder cleanRow = new StringBuilder();
						String[] columns = processedLine.split(";");
						for(int i = 0; i < columns.length; i++) {
							String cleaned = columns[i];
							for(Processor<String> proc : columnProcessors.get(i))
								cleaned = proc.process(cleaned);
							cleanRow.append(cleaned);
							cleanRow.append(";");
						}
						cleanRow.deleteCharAt(cleanRow.length() - 1);
						writer.write(cleanRow.toString());
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

	private String writeHeader(BufferedWriter writer, File headerFile) throws IOException {
		BufferedReader reader = new BufferedReader(new FileReader(headerFile));
		String header = reader.readLine();
		reader.close();
		writer.write(header);
		writer.newLine();
		
		return header;
	}

	private String removeLeadingJunk(String line, String startSignal) {
		int startIndex = line.indexOf(startSignal);
		// start behind start signal
		return line.substring(startIndex + startSignal.length());
	}
	
	private List<List<Processor<String>>> setUpColumnValidation(String header) {
		String[] columnNames = header.split(";");
		List<List<Processor<String>>> processors = new ArrayList<List<Processor<String>>>(columnNames.length);
		for(int columnIndex = 0; columnIndex < columnNames.length; columnIndex++) {
			for(final Column c : _table.getAllColumns()) {
				if(c.getName().equalsIgnoreCase(columnNames[columnIndex])) {
					List<Processor<String>> columnProcessors = _processors.stream()
							.filter(p -> p.getColumnType() == TeradataColumnType.ANY || p.getColumnType().getLabel().equalsIgnoreCase(c.getType()))
							.collect(Collectors.toList());
					processors.add(columnProcessors);
					continue;
				}
			}
		}
		
		return processors;
	}
}
