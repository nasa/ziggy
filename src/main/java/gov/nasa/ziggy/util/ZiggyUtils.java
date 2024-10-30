package gov.nasa.ziggy.util;

import static com.google.common.base.Preconditions.checkNotNull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.module.PipelineException;

/**
 * Contains static methods that don't belong anywhere else.
 *
 * @author Bill Wohler
 */
public class ZiggyUtils {
    private static final Logger log = LoggerFactory.getLogger(ZiggyUtils.class);

    @FunctionalInterface
    public interface ThrowingSupplier<T> {
        T get() throws Exception;
    }

    /**
     * Tries to obtain a value from the provided supplier until a value is returned or the maximum
     * number of tries is attempted.
     * <p>
     * The {@code pauseMillis} should be chosen so that it is short enough to keep this call timely,
     * but not too short to saturate the CPU. The value of {@code tries} should then be chosen so
     * that this method gives up after the maximum time the supplier should return under normal
     * circumstances.
     *
     * @param <T> The type this method and the supplier return
     * @param message The non-null log message with "(take 1/25)", for example, appended
     * @param tries The maximum number of tries to attempt obtaining the value from the supplier
     * @param pauseMillis The time to pause between each attempt to obtain the value from the
     * supplier, in milliseconds
     * @param supplier A non-null lambda that returns a value, which may or may not throw an
     * exception
     * @return The value that the supplier returns
     * @throws PipelineException If the supplier never returns a value in the alloted time
     */
    public static <T> T tryPatiently(String message, int tries, long pauseMillis,
        ThrowingSupplier<T> supplier) {

        checkNotNull(message, "message");
        checkNotNull(supplier, "supplier");

        for (int i = 1; i <= tries; i++) {
            log.info("{} (take {}/{})", message, i, tries);
            try {
                return supplier.get();
            } catch (Exception e) {
                if (i == tries) {
                    throw new PipelineException(e);
                }
                try {
                    Thread.sleep(pauseMillis);
                } catch (InterruptedException interrupt) {
                    Thread.currentThread().interrupt();
                    return null;
                }
            }
        }
        return null;
        // In your house, I long to be
        // Room by room, patiently
        // I'll wait for you there
        // Like a stone
        // I'll wait for you there
        // Alone
        // Audioslave, "Like a Stone"
    }
}
