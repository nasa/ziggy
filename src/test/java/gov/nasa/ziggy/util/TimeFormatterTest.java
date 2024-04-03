package gov.nasa.ziggy.util;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class TimeFormatterTest {

    private static final double TIME_HOURS = 12.0 + 34.0 / 60 + 56.0 / 3600;
    private static final int TIME_SECONDS = 12 * 3600 + 34 * 60 + 56;
    private static final String TIME_STRING = "12:34:56";
    private static final String TIME_STRING_NO_SECONDS = "12:34";

    private static final int ZERO_TIME = 0;
    private static final String ZERO_TIME_STRING = "0:00:00";
    private static final String ZERO_TIME_STRING_NO_SECONDS = "0:00";

    @Test(expected = NullPointerException.class)
    public void testNullTimeStringHhMmSsToTimeInHours() {
        TimeFormatter.timeStringHhMmSsToTimeInHours(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testEmptyTimeStringHhMmSsToTimeInHours() {
        TimeFormatter.timeStringHhMmSsToTimeInHours("");
    }

    @Test
    public void testTimeStringHhMmSsToTimeInHours() {
        assertEquals(TIME_HOURS, TimeFormatter.timeStringHhMmSsToTimeInHours(TIME_STRING), 0.0001);
        assertEquals(ZERO_TIME, TimeFormatter.timeStringHhMmSsToTimeInHours(ZERO_TIME_STRING),
            0.0001);
    }

    @Test(expected = NullPointerException.class)
    public void testNullTimeStringHhMmSsToTimeInSeconds() {
        TimeFormatter.timeStringHhMmSsToTimeInSeconds(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testEmptyTimeStringHhMmSsToTimeInSeconds() {
        TimeFormatter.timeStringHhMmSsToTimeInSeconds("");
    }

    @Test
    public void testTimeStringHhMmSsToTimeInSeconds() {
        assertEquals(TIME_SECONDS, TimeFormatter.timeStringHhMmSsToTimeInSeconds(TIME_STRING),
            0.0001);
        assertEquals(ZERO_TIME, TimeFormatter.timeStringHhMmSsToTimeInSeconds(ZERO_TIME_STRING),
            0.0001);
    }

    @Test
    public void testTimeInHoursToStringHhMmSs() {
        assertEquals(TIME_STRING, TimeFormatter.timeInHoursToStringHhMmSs(TIME_HOURS));
        assertEquals(ZERO_TIME_STRING, TimeFormatter.timeInHoursToStringHhMmSs(ZERO_TIME));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNegativeTimeInHoursToStringHhMmSs() {
        assertEquals(TIME_STRING, TimeFormatter.timeInHoursToStringHhMmSs(-1.5));
    }

    @Test
    public void testTimeInSecondsToStringHhMmSs() {
        assertEquals(TIME_STRING, TimeFormatter.timeInSecondsToStringHhMmSs(TIME_SECONDS));
        assertEquals(ZERO_TIME_STRING, TimeFormatter.timeInSecondsToStringHhMmSs(ZERO_TIME));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNegativeTimeInSecondsToStringHhMmSs() {
        assertEquals(TIME_STRING, TimeFormatter.timeInSecondsToStringHhMmSs(-3661));
    }

    @Test(expected = NullPointerException.class)
    public void testNullStripSeconds() {
        TimeFormatter.stripSeconds(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testEmptyStripSeconds() {
        TimeFormatter.stripSeconds("");
    }

    @Test
    public void testStripSeconds() {
        assertEquals(TIME_STRING_NO_SECONDS, TimeFormatter.stripSeconds(TIME_STRING));
        assertEquals(TIME_STRING_NO_SECONDS, TimeFormatter.stripSeconds(TIME_STRING_NO_SECONDS));
        assertEquals(ZERO_TIME_STRING_NO_SECONDS, TimeFormatter.stripSeconds(ZERO_TIME_STRING));
        assertEquals(ZERO_TIME_STRING_NO_SECONDS,
            TimeFormatter.stripSeconds(ZERO_TIME_STRING_NO_SECONDS));
    }
}
