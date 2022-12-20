package gov.nasa.ziggy.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.metrics.CounterMetric;
import gov.nasa.ziggy.metrics.IntervalMetric;
import gov.nasa.ziggy.metrics.IntervalMetricKey;

/**
 * This class provides retry logic for a specified {@link Retryable} object.
 * <p>
 * Assumes that the call method of the provided {@link Retryable} throws an {@link Exception} in the
 * case of a failure.
 *
 * @author Todd Klaus
 */
public class Retry<V> {
    private static final Logger log = LoggerFactory.getLogger(Retry.class);

    private int maxRetries = -1; // default infinite
    private long retryIntervalMillis = 5000;
    private String metricPrefix = "Retry.";

    public Retry() {
    }

    public Retry(int maxRetries) {
        this.maxRetries = maxRetries;
    }

    public Retry(int maxRetries, long retryIntervalMillis) {
        this.maxRetries = maxRetries;
        this.retryIntervalMillis = retryIntervalMillis;
    }

    /**
     * Execute up to maxRetries times
     *
     * @param r
     */
    public V execute(Retryable<V> r) throws Exception {
        int currentRetry = 0;
        Exception exception = null;

        while (maxRetries == -1 || currentRetry <= maxRetries) {
            log.info("retries: " + currentRetry + "/" + maxRetries);

            CounterMetric.increment(metricPrefix + "execute.attemptCount");
            exception = null;
            IntervalMetricKey key = IntervalMetric.start();
            try {
                V result = r.call(currentRetry);
                CounterMetric.increment(metricPrefix + "execute.successCount");
                return result;
            } catch (Exception e) {
                log.warn("currentRetry=" + currentRetry + ", caught e=" + e);
                CounterMetric.increment(metricPrefix + "execute.failureCount");

                currentRetry++;
                exception = e;
                try {
                    Thread.sleep(retryIntervalMillis);
                } catch (InterruptedException ignore) {
                }
            } finally {
                IntervalMetric.stop(metricPrefix + "execute.execTimeMillis", key);
            }
        }

        if (exception != null) {
            throw exception;
        }
        return null;
    }

    public String getMetricPrefix() {
        return metricPrefix;
    }

    public void setMetricPrefix(String metricPrefix) {
        this.metricPrefix = metricPrefix;
    }
}
