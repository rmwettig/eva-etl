package de.ingef.eva.etl.stage;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import de.ingef.eva.data.RowElement;
import de.ingef.eva.etl.Row;
import de.ingef.eva.utility.CsvWriter;
import lombok.extern.log4j.Log4j2;

@Log4j2
public class Output {
	private final BlockingQueue<Row> input;
	private final Row pill;
	private final Path root;
	
	private ExecutorService threadPool;
	private Thread transformEntry;
	private Map<String,Path> outputTargets;
	
	public Output (Row poisonPill, BlockingQueue<Row> source, Path outputRoot) {
		pill = poisonPill;
		input = source;
		root = outputRoot;
	}
	
	public boolean initialize(int queueSize, int threadCount) {
		threadPool = Executors.newFixedThreadPool(threadCount);
		try {
			outputTargets = createOutputTargets();
		} catch (IOException e) {
			log.error("Could not create output directories.", e);
		}
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
				Map<String,CsvWriter> writers = new HashMap<>();
				try {
					while(true) {
						Row row = input.take();
						if(row == pill)
							break;
						//FIXME this means likely parallel writing to the same file !!
						CompletableFuture.supplyAsync(
							() -> {
								try {
									String dataset = null;
									if(isDestatis(row)) {
										dataset = "destatis"; 
									} else if (matchesIk(row, "108036123")) {
										dataset = "bosch";
									} else if(matchesIk(row, "101931440") || matchesIk(row, "102137985") || matchesIk(row, "101922757")) {
										dataset = "salzgitter";
									}
									if(dataset == null)
										return null;
									CsvWriter writer = getOrCreateWriter(row.getDb() + "_" + row.getTable() + ".csv", writers, "destatis");
									row.getColumns().forEach( e -> writer.addEntry(e.getContent()));
									writer.writeLine();
								} catch (IOException e) {
									throw new RuntimeException("Could not write row '" + row.getTable() + "'", e);
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
				} finally {
					for(String dataset : writers.keySet())
						try {
							writers.get(dataset).close();
						} catch (IOException e) {
							log.error("Could close output for dataset '{}'", dataset);
							continue;
						}
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
	
	private boolean isDestatis(Row row) {
		if(!row.getDb().contains("FDB"))
			return false;
		Map<String,Integer> column2Index = row.getColumnName2Index();
		if(!column2Index.containsKey("flag_destatis"))
			return false;
		List<RowElement> columns = row.getColumns();
		if(!columns.get(column2Index.get("flag_destatis")).getContent().equalsIgnoreCase("1"))
			return false;
		return true;
	}
	
	private CsvWriter getOrCreateWriter(String outFile, Map<String,CsvWriter> writers, String dataset) throws IOException {
		CsvWriter writer = null;
		if(writers.containsKey(dataset)) {
			writer = writers.get(dataset);
		} else {
			writer = new CsvWriter(outputTargets.get(dataset).resolve(outFile).toFile());
			writer.open();
			writers.put(dataset, writer);
		}
		return writer;
	}
	
	private boolean matchesIk(Row row, String ik) {
		if(!row.getDb().equalsIgnoreCase("ADB"))
			return false;
		Map<String,Integer> columnIndices = row.getColumnName2Index();
		if(!columnIndices.containsKey("h2ik"))
			return false;
		int index = columnIndices.get("h2ik");
		if(!row.getColumns().get(index).getContent().equalsIgnoreCase(ik))
			return false;
		
		return true;
	}
}
