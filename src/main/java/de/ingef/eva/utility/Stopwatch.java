package de.ingef.eva.utility;

public class Stopwatch {
	private long startTime = 0;
	private long stopTime = 0;
	
	public void start() {
		startTime = System.currentTimeMillis();
	}
	
	public void stop() {
		stopTime = System.currentTimeMillis();
	}
	
	public String createReadableDelta() {
		StringBuffer deltaString = new StringBuffer();
		long delta = stopTime - startTime;

		long secondsInMilli = 1000;
		long minutesInMilli = secondsInMilli * 60;
		long hoursInMilli = minutesInMilli * 60;
		long daysInMilli = hoursInMilli * 24;
		
		long days = delta / daysInMilli;
		delta = delta % daysInMilli;
		
		long hours = delta / hoursInMilli;
		delta = delta % hoursInMilli;
		
		long minutes = delta / minutesInMilli;
		delta = delta % minutesInMilli;
		
		long seconds = delta / secondsInMilli;
		
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
}
