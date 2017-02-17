package de.ingef.eva.async;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

import org.apache.logging.log4j.Logger;

public class AsyncWriter implements Runnable {

	private String _outfile;
	private Iterable<String> _rows;
	private Logger _logger;
	public AsyncWriter(String outfile, Iterable<String> rows, Logger logger)
	{
		_outfile = outfile;
		_rows = rows;
		_logger = logger;
	}
	
	@Override
	public void run() {
		try(
				BufferedWriter writer = new BufferedWriter(new FileWriter(_outfile));
			)
		{
			for(String row : _rows)
			{
				writer.write(row);
				writer.newLine();
			}
		} catch (IOException e) {
			if(_logger != null)
				_logger.error("Could not write {}.\nReason: {}", _outfile, e.getMessage());
			else
				e.printStackTrace();
		}
	}

}
