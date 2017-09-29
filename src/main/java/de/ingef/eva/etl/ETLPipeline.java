package de.ingef.eva.etl;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;

import de.ingef.eva.configuration.Configuration;
import de.ingef.eva.etl.stage.Export;
import de.ingef.eva.etl.stage.Filter;

public class ETLPipeline {

	private BlockingQueue<Row> raw;
	private BlockingQueue<Row> filtered;
	private BlockingQueue<Row> transformed;
	
	private ExecutorService transformThreads;
	private ExecutorService outputThreads;
	
	private final Row poisonPill;
	private Configuration config;
	
	private Export exportStage;
	private Filter filterStage;
	
	public ETLPipeline() {
		poisonPill = new Row("POISON", "POISON", null, null);
	}
	
	public boolean initalize(Configuration configuration) {
		//FIXME read queue size from config as well as thread pool sizes
		int queueSize = 100_000;
		initializeQueues(queueSize);
		config = configuration;
		
		exportStage = new Export(poisonPill);
		exportStage.initialize(queueSize, 2);
		raw = exportStage.getOutput();
		
		filterStage = new Filter(poisonPill, raw);
		filterStage.initialize(queueSize, 2);
		filtered = filterStage.getOutput();
		
		return true;
	}

	private void initializeQueues(int queueSize) {
		raw = new ArrayBlockingQueue<>(queueSize);
		filtered = new ArrayBlockingQueue<>(queueSize);
		transformed = new ArrayBlockingQueue<>(queueSize);
	}	
}
