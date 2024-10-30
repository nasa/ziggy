package gov.nasa.ziggy.pipeline.definition;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import java.text.ParseException;

import org.junit.Test;

import gov.nasa.ziggy.util.SystemProxy;

/**
 * Performs unit tests for {@link ExecutionClock}.
 *
 * @author PT
 * @author Bill Wohler
 */
public class ExecutionClockTest {

    private static final long MILLIS_PER_SECOND = 1000L;
    private static final int SECONDS_PER_MINUTE = 60;
    private static final int MINUTES_PER_HOUR = 60;

    @Test
    public void testStartExecutionClockOnly() {
        ExecutionClock executionClock = new ExecutionClock();
        assertEquals("-", executionClock.toString());

        // 00:00:01
        SystemProxy.setUserTime(1 * MILLIS_PER_SECOND);
        executionClock.start();
        assertTrue(executionClock.isRunning());

        // 01:01:01
        SystemProxy
            .setUserTime((1 * MINUTES_PER_HOUR * SECONDS_PER_MINUTE + 1 * SECONDS_PER_MINUTE + 1)
                * MILLIS_PER_SECOND);
        assertEquals("01:01:00", executionClock.toString());
    }

    @Test
    public void testStopExecutionClock() {
        ExecutionClock executionClock = new ExecutionClock();

        // 00:00:01
        SystemProxy.setUserTime(1 * MILLIS_PER_SECOND);
        executionClock.start();
        assertTrue(executionClock.isRunning());

        // 00:01:00
        SystemProxy.setUserTime(1 * SECONDS_PER_MINUTE * MILLIS_PER_SECOND);
        executionClock.stop();
        assertFalse(executionClock.isRunning());
        assertEquals("00:00:59", executionClock.toString());
    }

    @Test
    public void testStopExecutionClockTwice() throws ParseException {
        ExecutionClock executionClock = new ExecutionClock();

        // 00:00:01
        SystemProxy.setUserTime(1 * MILLIS_PER_SECOND);
        executionClock.start();
        assertTrue(executionClock.isRunning());

        // 00:00:01
        SystemProxy.setUserTime(1 * MILLIS_PER_SECOND);
        assertEquals("00:00:00", executionClock.toString());

        // 00:01:00
        SystemProxy.setUserTime(1 * SECONDS_PER_MINUTE * MILLIS_PER_SECOND);
        executionClock.stop();
        assertFalse(executionClock.isRunning());
        assertEquals("00:00:59", executionClock.toString());

        // 00:01:30
        SystemProxy.setUserTime((1 * SECONDS_PER_MINUTE + 30) * MILLIS_PER_SECOND);
        executionClock.start();
        assertTrue(executionClock.isRunning());

        // 00:01:45
        SystemProxy.setUserTime((1 * SECONDS_PER_MINUTE + 45) * MILLIS_PER_SECOND);
        assertEquals("00:01:14", executionClock.toString());

        // 00:02:30
        SystemProxy.setUserTime((2 * SECONDS_PER_MINUTE + 30) * MILLIS_PER_SECOND);
        executionClock.stop();
        assertFalse(executionClock.isRunning());
        assertEquals("00:01:59", executionClock.toString());
    }

    @SuppressWarnings("unlikely-arg-type")
    @Test
    public void testHashCodeEquals() {
        ExecutionClock executionClock1 = new ExecutionClock();
        ExecutionClock executionClock2 = new ExecutionClock();
        assertEquals(executionClock1.hashCode(), executionClock2.hashCode());
        assertTrue(executionClock1.equals(executionClock1));
        assertTrue(executionClock1.equals(executionClock2));

        executionClock2.start();
        assertNotEquals(executionClock1.hashCode(), executionClock2.hashCode());
        assertFalse(executionClock1.equals(executionClock2));

        executionClock2.stop();
        executionClock2.stop();
        executionClock2.start();
        executionClock2.start();
        assertNotEquals(executionClock1.hashCode(), executionClock2.hashCode());
        assertFalse(executionClock1.equals(executionClock2));

        assertFalse(executionClock1.equals(null));
        assertFalse(executionClock1.equals("a string"));
    }
}
