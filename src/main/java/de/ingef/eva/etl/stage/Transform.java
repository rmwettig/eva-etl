package de.ingef.eva.etl.stage;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import de.ingef.eva.data.RowElement;
import de.ingef.eva.data.SimpleRowElement;
import de.ingef.eva.data.TeradataColumnType;
import de.ingef.eva.etl.Row;
import lombok.Getter;
import lombok.extern.log4j.Log4j2;

@Log4j2
public class Transform {
	private final BlockingQueue<Row> input;
	private final Row pill;
	
	private ExecutorService threadPool;
	@Getter
	private BlockingQueue<Row> output;
	private Thread transformEntry;
	
	public Transform (Row poisonPill, BlockingQueue<Row> source) {
		pill = poisonPill;
		input = source;
	}
	
	public boolean initialize(int queueSize, int threadCount) {
		threadPool = Executors.newFixedThreadPool(threadCount);
		output = new ArrayBlockingQueue<>(queueSize);
		return true;
	}
	
	public void start() {
		transformEntry = createTransformMain();
		transformEntry.start();
	}
	
	public boolean isRunning() {
		return transformEntry.isAlive();
	}
	
	/**
	 * Represents the single entry point to the transform stage
	 * @return
	 */
	private Thread createTransformMain() {
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
					output.put(pill);
					threadPool.shutdown();
					threadPool.awaitTermination(1, TimeUnit.DAYS);
				} catch (InterruptedException | RuntimeException e) {
					e.printStackTrace();
				}
			}			
		});
	}
	
	private boolean isFDB(Row row) {
		return row.getDb().contains("FDB");
	}
	
	private Row addPseudoH2ik(Row row) {
		List<RowElement> transformedColumns = transformColumns(row);
		Map<String, Integer> transformedNames2Index = transformColumnIndices(row);
		
		return new Row(row.getDb(), row.getTable(), transformedColumns, transformedNames2Index);
	}

	private List<RowElement> transformColumns(Row row) {
		List<RowElement> originalColumns = row.getColumns();
		List<RowElement> transformedColumns = new ArrayList<>(originalColumns.size() + 1);
		transformedColumns.add(new SimpleRowElement("999999999", TeradataColumnType.CHARACTER));
		transformedColumns.addAll(originalColumns);
		return transformedColumns;
	}

	private Map<String, Integer> transformColumnIndices(Row row) {
		Map<String,Integer> names2Index = row.getColumnName2Index();
		Map<String,Integer> transformedNames2Index = new HashMap<>(names2Index.size() + 1);
		transformedNames2Index.put("H2IK", 0);
		for(String column : names2Index.keySet()) {
			transformedNames2Index.put(column, names2Index.get(column) + 1);
		}
		return transformedNames2Index;
	}
}
