package gov.nasa.ziggy.util;

/**
 * Utilities for converting time from one format to another.
 *
 * @author PT
 */
public class TimeFormatter {

    /**
     * Convert a string in HH:mm:SS format to a double-precision number of hours.
     */
    public static double timeStringHhMmSsToTimeInHours(String timeString) {
        String[] wallTimeChunks = timeString.split(":");
        return Double.parseDouble(wallTimeChunks[0]) + Double.parseDouble(wallTimeChunks[1]) / 60
            + Double.parseDouble(wallTimeChunks[2]) / 3600;
    }

    /**
     * Convert a string in HH:mm:SS format to a double-precision number of seconds.
     */
    public static double timeStringHhMmSsToTimeInSeconds(String timeString) {
        String[] wallTimeChunks = timeString.split(":");
        return Double.parseDouble(wallTimeChunks[0]) * 3600
            + Double.parseDouble(wallTimeChunks[1]) * 60 + Double.parseDouble(wallTimeChunks[2]);
    }

    /**
     * Convert a double-precision number of hours to a string in HH:mm:SS format.
     */
    public static String timeInHoursToStringHhMmSs(double timeHours) {
        StringBuilder sb = new StringBuilder();
        double wallTimeHours = Math.floor(timeHours);
        sb.append((int) wallTimeHours);
        sb.append(":");
        double wallTimeMinutes = Math.floor(60 * (timeHours - wallTimeHours));
        sb.append(twoDigitString(wallTimeMinutes));
        sb.append(":");
        double wallTimeSeconds = Math
            .floor(3600 * (timeHours - wallTimeHours - wallTimeMinutes / 60));
        sb.append(twoDigitString(wallTimeSeconds));
        return sb.toString();
    }

    public static String timeInSecondsToStringHhMmSs(int timeSeconds) {
        double timeHours = (double) timeSeconds / 3600;
        return timeInHoursToStringHhMmSs(timeHours);
    }

    private static String twoDigitString(double value) {
        return String.format("%02d", (int) value);
    }

}
