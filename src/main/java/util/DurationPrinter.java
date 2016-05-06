package util;

import java.util.concurrent.TimeUnit;

public class DurationPrinter {

	public static String formatDuration(long milliseconds) {

		long hours = TimeUnit.MILLISECONDS.toHours(milliseconds);
		milliseconds -= TimeUnit.HOURS.toMillis(hours);

		long minutes = TimeUnit.MILLISECONDS.toMinutes(milliseconds);
		milliseconds -= TimeUnit.MINUTES.toMillis(minutes);

		long seconds = TimeUnit.MILLISECONDS.toSeconds(milliseconds);

		String formatted;
		
		if (hours != 0) {
			formatted = String.format("%02d hours, %02d minutes and %02d seconds", hours, minutes, seconds);
		} else if (minutes != 0) {
			formatted = String.format("%02d minutes and %02d seconds", minutes, seconds);
		} else if (seconds != 0) {
			formatted = String.format("%02d seconds", seconds);
		} else {
			formatted = "0 seconds";
		}

		return formatted;
	}
	
	public static void printElapsedTime(long startTime) {
		printElapsedTimeWithMessage(startTime, "Time from Beginning");
	}
	
	public static void printElapsedTimeWithMessage(long startTime, String message) {
		long endTime = System.currentTimeMillis();
		long elapsedTime = endTime - startTime;

		String formattedElapsedTime = DurationPrinter.formatDuration(elapsedTime);
		System.out.println("##########################################################");
		System.out.println(message + ": " + formattedElapsedTime);
		System.out.println("##########################################################");
	}
}