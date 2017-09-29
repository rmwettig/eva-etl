package de.ingef.eva.etl.stage;

import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import de.ingef.eva.etl.Row;
import lombok.Getter;
import lombok.extern.log4j.Log4j2;

@Log4j2
public class Filter {
	
	private final BlockingQueue<Row> input;
	private final Row pill;
	
	private ExecutorService threadPool;
	@Getter
	private BlockingQueue<Row> output;
	private Thread filterEntry;
	
	private Pattern fgPattern = Pattern.compile("[0-9]{2}");
	
	public Filter (Row poisonPill, BlockingQueue<Row> source) {
		pill = poisonPill;
		input = source;
	}
	
	public boolean initialize(int queueSize, int threadCount) {
		threadPool = Executors.newFixedThreadPool(threadCount);
		output = new ArrayBlockingQueue<>(queueSize);
		return true;
	}
	
	public void start() {
		filterEntry = createFilterMain();
		filterEntry.start();
	}
	
	public boolean isRunning() {
		return filterEntry.isAlive();
	}
	
	/**
	 * Represents the single entry point to the filtering stage
	 * @return
	 */
	private Thread createFilterMain() {
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
									if(isFGTwoDigitNumber(row))
										output.put(row);
									else
										log.warn("Invalid column 'Fachgruppe': {}", row.getColumns().stream().map(e -> e.getContent()).collect(Collectors.joining(", ")));
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
	
	private boolean isFGTwoDigitNumber(Row row) {
		Map<String, Integer> names2Index = row.getColumnName2Index();
		//if 'Fachgruppe' does not exist skip
		if(!names2Index.containsKey("fg"))
			return true;
		
		int index = names2Index.get("fg");
		String value = row.getColumns().get(index).getContent();
		return fgPattern.matcher(value).matches();
	}
	
}
