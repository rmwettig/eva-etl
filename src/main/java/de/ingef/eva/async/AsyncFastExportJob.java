package de.ingef.eva.async;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.concurrent.CountDownLatch;

import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;

@Log4j2
@RequiredArgsConstructor
public class AsyncFastExportJob implements Runnable {
	
	private final String errorFile;
	private final String fxFile;
	private final CountDownLatch cdl;
	
	//TODO add retry logic
	
	@Override
	public void run() {
		//wrap files into quotation marks to handle paths with spaces
		String cmd = "fexp -e \"" + errorFile + "\" < \"" + fxFile + "\"";
		//actual command must be quoted
		cmd = "\"" + cmd + "\"";
		ProcessBuilder pb = new ProcessBuilder("cmd.exe", "/c" , cmd);
		Process p = null;
		try {
			p = pb.start();
			//avoid hanging process if no process messages are read
			startStreamReaderThread(p.getInputStream());
			startStreamReaderThread(p.getErrorStream());
			p.waitFor();
		} catch (IOException e) {
			log.error("Could not dump query result: {}", e.getMessage());
		} catch (InterruptedException e) {
			log.error(e);
		} finally {
			//stop the FastExport subprocess
			//if it was created and is still alive after dumping
			if(p != null && p.isAlive())
				p.destroy();
			cdl.countDown();
		}
	}
	
	/**
	 * Creates a separate daemon thread for reading process console output
	 * @param stream process console stream
	 */
	private void startStreamReaderThread(InputStream stream) {
		Thread t = new Thread(new Runnable() {
			
			@Override
			public void run() {
				String line = null;
				try (BufferedReader reader = new BufferedReader(new InputStreamReader(stream));) {
					while ((line = reader.readLine()) != null) {
						System.out.println(line);
					} 
				} catch (IOException e) {
					log.error("Could not read console output from FastExport process: ", e);
				}
			}
		});
		t.setDaemon(true);
		t.start();
	}

}
