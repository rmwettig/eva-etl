package de.ingef.eva.etl.stage;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import de.ingef.eva.etl.Row;

public class Output {
	private final BlockingQueue<Row> input;
	private final Row pill;
	private final Path root;
	
	private ExecutorService threadPool;
	private Thread transformEntry;
		
	public Output (Row poisonPill, BlockingQueue<Row> source, Path outputRoot) {
		pill = poisonPill;
		input = source;
		root = outputRoot;
	}
	
	public boolean initialize(int queueSize, int threadCount) {
		threadPool = Executors.newFixedThreadPool(threadCount);
		return true;
	}
	
	public void start() {
		transformEntry = createOutputMain();
		transformEntry.start();
	}
	
	/**
	 * Represents the single entry point to the transform stage
	 * @return
	 */
	private Thread createOutputMain() {
		return new Thread(new Runnable() {
			
			@Override
			public void run() {
				try {
					while(true) {
						Row row = input.take();
						if(row == pill)
							break;
						CompletableFuture.supplyAsync(
							() -> {
								try {
									//FIXME generalize filters
									if(isFDB(row))
										output.put(addPseudoH2ik(row));
									else
										output.put(row);
								} catch (InterruptedException e) {
									throw new RuntimeException("Could not apply filter on row '" + row.getTable() + "'", e);
								}
								return null;
							},
							threadPool
						);
					}
					threadPool.shutdown();
					threadPool.awaitTermination(1, TimeUnit.DAYS);
				} catch (InterruptedException | RuntimeException e) {
					e.printStackTrace();
				}
			}			
		});
	}
	
	private Map<String,Path> createOutputTargets() throws IOException {
		Map<String,Path> targets = new HashMap<>();
		createDirectoryIfNotExists(root);
		targets.put("destatis", createSubsetPath("FDB", "destatis"));
		targets.put("bosch", createSubsetPath("ADB", "Bosch"));
		targets.put("salzgitter", createSubsetPath("ADB", "Salzgitter"));
		return targets;
	}
	
	private Path createSubsetPath(String source, String dataset) throws IOException {
		Path p = Paths.get(source, dataset);
		createDirectoryIfNotExists(p);
		return root.resolve(p);
	}
	
	private void createDirectoryIfNotExists(Path p) throws IOException {
		if(!Files.exists(p))
			Files.createDirectories(p);
	}
}
