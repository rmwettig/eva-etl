package de.ingef.eva.services;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import de.ingef.eva.error.TaskExecutionException;
import de.ingef.eva.tasks.Task;
import lombok.Getter;

public class TaskRunnerTest {

	@Getter
	private static class Counter extends Task<Integer> {
		private int callCount = 0;
		public Counter() {
			super("JUnit Test-Task", "");
		}
		public Integer execute() {
			callCount++;
			return 1337;
		}
	}
	
	@Getter
	private static class CounterWithExceptions extends Task<Integer> {
		private int callCount = 0;
		private final int numberOfExceptions;
		
		public CounterWithExceptions(int exceptionCount) {
			super("JUnit Test-Task with Exceptions", "");
			numberOfExceptions = exceptionCount;
		}
		
		public Integer execute() {
			if(callCount++ < numberOfExceptions)
				throw new TaskExecutionException("Call count < 2");
			return 1337;
		}
	}
	
	@Test
	public void runsExactlyOnce() {
		TaskRunner runner = new TaskRunner(1);
		Counter c = new Counter();
		runner.run(c).join();
		assertEquals(1, c.getCallCount());
	}
	
	@Test
	public void repeatTaskExactlyTwiceOnError() {
		TaskRunner runner = new TaskRunner(1);
		CounterWithExceptions c = new CounterWithExceptions(1);
		runner.run(c, 2).join();
		assertEquals(2, c.getCallCount());
	}

	@Test(expected=RuntimeException.class)
	public void completelyFailsIfNumberOfRepetitionsAreExceeded() {
		TaskRunner runner = new TaskRunner(1);
		CounterWithExceptions c = new CounterWithExceptions(3);
		runner.run(c, 2).join();
	}
	
}
