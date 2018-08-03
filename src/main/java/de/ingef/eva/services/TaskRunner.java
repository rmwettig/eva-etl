package de.ingef.eva.services;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import de.ingef.eva.error.TaskExecutionException;
import de.ingef.eva.tasks.Task;
import lombok.extern.log4j.Log4j2;

/**
 * This class allows to execute arbitrary tasks in parallel.
 * The spawned threads are daemon threads which are stopped along with the main java process.
 * 
 * @author Martin.Wettig
 *
 */
@Log4j2
public class TaskRunner {

	private ExecutorService threadpool;
	
	public TaskRunner(int poolSize) {
		threadpool = Executors.newFixedThreadPool(poolSize, new ThreadFactory() {
			
			@Override
			public Thread newThread(Runnable r) {
				Thread t = new Thread(r);
				t.setDaemon(true);
				return t;
			}
		});
	}
	
	/**
	 * execute the given task once
	 * @param task
	 * @return
	 */
	public <T> CompletableFuture<T> run(Task<T> task) {
		return CompletableFuture.supplyAsync(task::execute, threadpool);
	}
	
	/**
	 * retries the given task on failure
	 * @param task
	 * @param repetitions
	 * @return
	 */
	public <T> CompletableFuture<T> run(Task<T> task, int repetitions) {
		return CompletableFuture.supplyAsync(() -> {
			for(int repetition = 0; repetition < repetitions; repetition++) {
				log.info("Executing task '{}' (iteration {}): {}", task.getName(), repetition, task.getDescription());
				try {
					T result = task.execute();
					log.info("Task '{}' completed: {}", task.getName(), task.getDescription());
					return result;
				} catch (TaskExecutionException e) {
					log.error(e);
					continue;
				}
			}
			throw new RuntimeException("Maximum number of retries reached. Task '" + task.getName() + "' will not be retried.");
		}, threadpool);
	}
}
