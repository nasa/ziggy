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
 * <p>
 * Example:
 *
 * <pre>
 * boolean eventDetected = TestEventDetector.detectTestEvent(1000L,
 *     () -> Files.exists(Paths.get("foo.txt")));
 * </pre>
 *
 * will wait for up to 1000 milliseconds for the file "foo.txt" to appear in the working directory.
 * If the file appears within that time, eventDetected will be true, otherwise false.
 * <p>
 * The major benefit of using this rather than something like:
 *
 * <pre>
 * try {
 *     Thread.sleep(1000L);
 *     } catch (InterruptedException e} {
 *         // swallow exception
 *     }
 * boolean eventDetected = Files.exists(Paths.get("foo.txt"));
 * </pre>
 *
 * is that the {@link TestEventDetector} is not required to block execution for the full timeout
 * duration, which is what the {@link Thread#sleep(long)} will do. Also, it makes the code prettier
 * and easier to read.
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
