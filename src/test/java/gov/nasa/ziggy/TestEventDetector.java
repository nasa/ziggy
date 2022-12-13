package gov.nasa.ziggy;

import gov.nasa.ziggy.util.SystemTime;

/**
 * Provides a means for unit tests to wait for either a timeout or else a specified event.
 * <p>
 * The test event is specified by an implementation of {@link TestEventDefinition}, which is a
 * functional interface whose method, {@link TestEventDefinition#eventDetected()}, returns true if
 * the event of interest has occurred, false otherwise. The static method
 * {@link #detectTestEvent(long, TestEventDefinition)} combines the desired timeout, in
 * milliseconds, with the {@link TestEventDefinition} implementation. It will loop until either the
 * timeout has been exceeded or the specified event has been detected, and will return true if the
 * event was detected (false, obviously, if the timeout was exceeded).
 *
 * @author PT
 */
public class TestEventDetector {

    public static boolean detectTestEvent(long timeout, TestEventDefinition definition) {
        long startTime = SystemTime.currentTimeMillis();
        boolean eventDetected = false;
        while (SystemTime.currentTimeMillis() < startTime + timeout && !eventDetected) {
            eventDetected = definition.eventDetected();
        }
        return eventDetected;
    }

    @FunctionalInterface
    public interface TestEventDefinition {

        boolean eventDetected();
    }
}
