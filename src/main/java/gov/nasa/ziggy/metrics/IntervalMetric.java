package gov.nasa.ziggy.metrics;

import java.io.Serializable;
import java.util.concurrent.Callable;

import gov.nasa.ziggy.util.SystemTime;

/**
 * Metric to measure elapsed time
 *
 * @author Todd Klaus
 */
public class IntervalMetric extends ValueMetric implements Serializable {
    private static final long serialVersionUID = 6018286347911138007L;

    @FunctionalInterface
    public interface Perform {
        void apply() throws Exception;
    }

    public static void report(String metricName, Perform p) throws Exception {
        IntervalMetricKey metricKey = start();
        try {
            p.apply();
        } finally {
            stop(metricName, metricKey);
        }
    }

    /**
     * TODO: This calls System.currentTimeMillis() which is kind of slow.
     *
     * @return
     */
    public static IntervalMetricKey start() {
        return new IntervalMetricKey(SystemTime.currentTimeMillis());
    }

    /**
     * @param name
     * @param key
     */
    public static IntervalMetric stop(String name, IntervalMetricKey key) {
        long stopTime = SystemTime.currentTimeMillis();
        GlobalThreadMetrics<IntervalMetric> m = getIntervalMetric(name);
        m.getGlobalMetric().stop(key, stopTime);
        if (m.getThreadMetric() != null) {
            m.getThreadMetric().stop(key, stopTime);
        }
        return m.getThreadMetric();
    }

    /**
     * Convenience method for measuring the execution time of a block of code
     *
     * @param <V>
     * @param name
     * @param target
     */
    public static <V> V measure(String name, Callable<V> target) throws Exception {
        IntervalMetricKey key = IntervalMetric.start();

        try {
            return target.call();
        } finally {
            IntervalMetric.stop(name, key);
        }
    }

    /**
     * Convenience method for measuring the execution time of a block of code
     *
     * @param name
     * @param target
     */
    public static void measure(String name, Runnable target) throws Exception {
        IntervalMetricKey key = IntervalMetric.start();

        try {
            target.run();
        } finally {
            IntervalMetric.stop(name, key);
        }
    }

    protected IntervalMetric(String name) {
        super(name);
    }

    /**
     * @param name
     * @return
     */
    protected static GlobalThreadMetrics<IntervalMetric> getIntervalMetric(String name) {
        Metric globalMetric = Metric.getGlobalMetric(name);
        if (globalMetric == null || !(globalMetric instanceof IntervalMetric)) {
            globalMetric = Metric.addNewGlobalMetric(new IntervalMetric(name));
        }

        Metric threadMetric = null;
        if (Metric.threadMetricsEnabled()) {
            threadMetric = Metric.getThreadMetric(name);
            if (threadMetric == null || !(threadMetric instanceof IntervalMetric)) {
                threadMetric = addNewThreadMetric(new IntervalMetric(name));
            }
        }
        return new GlobalThreadMetrics<>((IntervalMetric) globalMetric,
            (IntervalMetric) threadMetric);
    }

    /**
     * Not synchronized because addValue() is
     *
     * @param key
     */
    protected void stop(IntervalMetricKey key, long systemCurrentTimeMillis) {
        addValue(systemCurrentTimeMillis - key.getStartTime());
    }

    @Override
    public synchronized ValueMetric makeCopy() {
        IntervalMetric copy = new IntervalMetric(name);
        copy.name = name;
        copy.sum = sum;
        copy.min = min;
        copy.max = max;
        copy.count = count;
        return copy;
    }

}
