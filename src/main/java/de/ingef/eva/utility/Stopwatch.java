package de.ingef.eva.utility;

public class Stopwatch {
	private long startTime = 0;
	private long stopTime = 0;
	
	public void start() {
		startTime = sampleTime();
	}
	
	public void stop() {
		stopTime = sampleTime();
	}
	
	public String createReadableDelta() {
		StringBuffer deltaString = new StringBuffer();
		long delta = stopTime - startTime;
		// 1.5h => 90 min => 5400s
		//conversion: 5400s / 60 = 90 for minutes, 5400s mod 60 for remaining seconds
		long minutes = delta / 60;
		long seconds = Math.floorMod(delta, 60);
		long hours = minutes / 60;
		minutes += Math.floorMod(minutes, 60);
		long days = hours / 24;
		hours += Math.floorMod(days, 24);
		
		if(days > 0) {
			deltaString.append(days + "d");
			deltaString.append(" ");
		}
		if(hours > 0) {
			deltaString.append(hours +"h");
			deltaString.append(" ");
		}
		if(minutes > 0) {
			deltaString.append(minutes + "min");
			deltaString.append(" ");
		}
		if(seconds > 0) {
			deltaString.append(seconds + "s");
			deltaString.append(" ");
		}
		
		return deltaString.toString();
	}
	
	private long sampleTime() {
		//samples time in seconds
		return System.currentTimeMillis() / 1000;
	}
}
