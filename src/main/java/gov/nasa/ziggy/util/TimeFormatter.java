package gov.nasa.ziggy.util;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Utilities for converting time from one format to another.
 *
 * @author PT
 */
public class TimeFormatter {

    private static final Pattern TIME_REGEXP = Pattern.compile("(\\d+:\\d+)(:\\d+)?");

    /**
     * Convert a string in HH:mm:SS format to a double-precision number of hours.
     */
    public static double timeStringHhMmSsToTimeInHours(String timeString) {
        checkNotNull(timeString, "timeString");
        checkArgument(!timeString.isEmpty(), "timeString can't be empty");

        String[] wallTimeChunks = timeString.split(":");
        return Double.parseDouble(wallTimeChunks[0]) + Double.parseDouble(wallTimeChunks[1]) / 60
            + Double.parseDouble(wallTimeChunks[2]) / 3600;
    }

    /**
     * Convert a string in HH:mm:SS format to a double-precision number of seconds.
     */
    public static double timeStringHhMmSsToTimeInSeconds(String timeString) {
        checkNotNull(timeString, "timeString");
        checkArgument(!timeString.isEmpty(), "timeString can't be empty");

        String[] wallTimeChunks = timeString.split(":");
        return Double.parseDouble(wallTimeChunks[0]) * 3600
            + Double.parseDouble(wallTimeChunks[1]) * 60 + Double.parseDouble(wallTimeChunks[2]);
    }

    /**
     * Convert a double-precision number of hours to a string in HH:mm:SS format.
     */
    public static String timeInHoursToStringHhMmSs(double timeHours) {
        checkArgument(timeHours >= 0, "timeHours can't be negative");

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
        checkArgument(timeSeconds >= 0, "timeSeconds can't be negative");

        double timeHours = (double) timeSeconds / 3600;
        return timeInHoursToStringHhMmSs(timeHours);
    }

    private static String twoDigitString(double value) {
        return String.format("%02d", (int) value);
    }

    /**
     * Given a time string such as 1:23:45, strip off the seconds so you're left with 1:23. If this
     * method is given 1:23, 1:23 is returned.
     *
     * @param timeString a string of the form hh:mm[:ss]
     * @return a string of the form hh:mm
     */
    public static String stripSeconds(String timeString) {
        checkNotNull(timeString, "timeString");
        checkArgument(!timeString.isEmpty(), "timeString can't be empty");

        Matcher matcher = TIME_REGEXP.matcher(timeString);
        if (matcher.find()) {
            return matcher.group(1);
        }
        return timeString;
    }
}
