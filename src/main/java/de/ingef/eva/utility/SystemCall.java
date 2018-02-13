package de.ingef.eva.utility;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class SystemCall {
	
	private final List<String> command;
	private Process p;
	
	public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
		return p.waitFor(timeout, unit);
	}
	
	public void execute(Consumer<String> progressCallback) throws IOException {
		p = new ProcessBuilder(command)
				.redirectErrorStream(true)
				.start();
		createAsyncOutputReader(p.getInputStream(), progressCallback).start();
	}
	
	private Thread createAsyncOutputReader(InputStream processOutput, Consumer<String> progressCallback) {
		Thread t = new Thread(new Runnable() {
			@Override
			public void run() {
				try (BufferedReader reader = new BufferedReader(new InputStreamReader(processOutput))) {
					String line = null;
					while((line = reader.readLine()) != null) {
						if(progressCallback != null)
							progressCallback.accept(line);
					}
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		});
		t.setDaemon(true);
		return t;
	}
}
