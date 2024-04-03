package gov.nasa.ziggy.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.Date;

import org.junit.Test;

public class TimeRangeTest {

    private static final long HOUR_MILLISECONDS = 60 * 60 * 1000;

    @SuppressWarnings("unlikely-arg-type")
    @Test
    public void testHashCodeEquals() {
        TimeRange timeRange = timeRange(12345);
        assertTrue(timeRange.equals(timeRange));
        assertFalse(timeRange.equals(null));
        assertFalse(timeRange.equals("a string"));

        assertTrue(timeRange(12345).equals(timeRange(12345)));
        assertFalse(timeRange(12345).equals(timeRange(54321)));

        assertEquals(timeRange(12345).hashCode(), timeRange(12345).hashCode());
        assertNotEquals(timeRange(12345).hashCode(), timeRange(54321).hashCode());
    }

    @Test
    public void testGetStartTimestamp() {
        assertEquals(new Date(12345), timeRange(12345).getStartTimestamp());
        assertNotEquals(new Date(54321), timeRange(12345).getStartTimestamp());

        assertNull(new TimeRange(null, null).getStartTimestamp());
    }

    @Test
    public void testGetEndTimestamp() {
        assertEquals(new Date(12345 + HOUR_MILLISECONDS), timeRange(12345).getEndTimestamp());
        assertNotEquals(new Date(54321 + HOUR_MILLISECONDS), timeRange(12345).getEndTimestamp());

        assertNull(new TimeRange(null, null).getEndTimestamp());
    }

    /** Returns a range from the startSeed to one hour after. */
    private TimeRange timeRange(long startSeed) {
        return new TimeRange(new Date(startSeed), new Date(startSeed + HOUR_MILLISECONDS));
    }
}
