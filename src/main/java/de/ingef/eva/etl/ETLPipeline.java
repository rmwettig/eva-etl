package de.ingef.eva.etl;

import java.util.Collection;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;

import de.ingef.eva.configuration.Configuration;
import de.ingef.eva.etl.stage.Export;
import de.ingef.eva.query.Query;

public class ETLPipeline {

	private BlockingQueue<Row> raw;
	private BlockingQueue<Row> filtered;
	private BlockingQueue<Row> transformed;
	
	
	private ExecutorService filterThreads;
	private ExecutorService transformThreads;
	private ExecutorService outputThreads;
	
	private final Row poisonPill;
	private Configuration config;
	
	private Export exportStage;
	
	public ETLPipeline() {
		poisonPill = new Row("POISON", "POISON", null);
	}
	
	public boolean initalize(Configuration configuration) {
		int queueSize = 100_000;
		initializeQueues(queueSize);
		config = configuration;
		
		exportStage = new Export(poisonPill);
		exportStage.initialize(queueSize, 2);
		raw = exportStage.getOutput();
		
		return true;
	}

	private void initializeQueues(int queueSize) {
		raw = new ArrayBlockingQueue<>(queueSize);
		filtered = new ArrayBlockingQueue<>(queueSize);
		transformed = new ArrayBlockingQueue<>(queueSize);
	}
	
	public void start(Collection<Query> queries) {
		startExportTasks(queries, raw, exportThreads);
	}

	private void startExportTasks(Collection<Query> queries, BlockingQueue<Row> output, ExecutorService threadPool) {
		
	}
	
}
