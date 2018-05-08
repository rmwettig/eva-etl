package de.ingef.eva.etl;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import lombok.Builder;
import lombok.Singular;

@Builder
public class WaitForCallbacks<T> {
	
	@Singular
	private List<CompletableFuture<T>> promises;
		
	public List<T> getResults() throws InterruptedException, ExecutionException {
		CompletableFuture<?>[] callbacks = new CompletableFuture[promises.size()];
		IntStream.range(0, promises.size()).forEach(i -> callbacks[i] = promises.get(i));
		CompletableFuture.allOf(callbacks).get();
		return promises
				.stream()
				.map(promise -> promise.join())
				.collect(Collectors.toList());
	}
}
