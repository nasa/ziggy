package gov.nasa.ziggy.util;

/**
 * @author Todd Klaus
 */
public interface Retryable<V> {
    /**
     * Computes a result, or throws an exception if unable to do so.
     *
     * @return computed result
     * @throws Exception if unable to compute a result
     */
    V call(int retryNumber) throws Exception;
}
