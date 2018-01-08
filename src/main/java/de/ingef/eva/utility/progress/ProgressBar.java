package de.ingef.eva.utility.progress;

import java.util.stream.Collectors;
import java.util.stream.IntStream;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

/**
 * Simple console progress bar
 * @author Martin.Wettig
 *
 */
public class ProgressBar {
	private static final int RENDER_MAXIMUM_LENGTH = 100;
	private final int maximum;
	private final float requiredProgressPerFill;
	private int current = 0;
	private int currentRenderFill = 0;
	
	
	public ProgressBar(int maximum) {
		this.maximum = maximum;
		requiredProgressPerFill = maximum / (float) RENDER_MAXIMUM_LENGTH;
	}
	
	@Getter
	@RequiredArgsConstructor
	private enum SYMBOL {
		BEGIN("["),
		END("]"),
		PROGRESS("="),
		PENDING(" ");
		
		private final String symbol;
	}
	
	/**
	 * indicates that progress was made. This method is thread-safe.
	 */
	public synchronized void increase() {
		++current;
		updateRenderProgress();
		print();
	}
	
	private void print() {
		System.out.print(assemble());
	}
	
	private String progress(int current) {
		return IntStream
				.range(0, RENDER_MAXIMUM_LENGTH)
				.mapToObj(i -> i <= currentRenderFill ? SYMBOL.PROGRESS : SYMBOL.PENDING)
				.map(symbol -> symbol.getSymbol())
				.collect(Collectors.joining());
	}
	
	private float calculatePercentage() {
		return current / (float) maximum * 100f;
	}
	
	private void updateRenderProgress() {
		currentRenderFill = (int) Math.floor(current / requiredProgressPerFill);
	}
	
	private String assemble() {
		String progress = progress(current);
		String details = String.format("(%d %% | %d/%d)", Math.round(calculatePercentage()), current, maximum);
		return SYMBOL.BEGIN.getSymbol() + progress + SYMBOL.END.getSymbol() + details + "\r";
	}
}
