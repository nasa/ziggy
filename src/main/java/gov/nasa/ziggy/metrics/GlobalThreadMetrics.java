package gov.nasa.ziggy.metrics;

import java.util.Objects;

/**
 * Container for a global metric and a corresponding thread metric.
 *
 * @author PT
 */
class GlobalThreadMetrics<T extends Metric> {

    private final T globalMetric;
    private final T threadMetric;

    public GlobalThreadMetrics(T globalMetric, T threadMetric) {
        this.globalMetric = globalMetric;
        this.threadMetric = threadMetric;
    }

    public T getGlobalMetric() {
        return globalMetric;
    }

    public T getThreadMetric() {
        return threadMetric;
    }

    @Override
    public int hashCode() {
        return Objects.hash(globalMetric, threadMetric);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        GlobalThreadMetrics other = (GlobalThreadMetrics) obj;
        return Objects.equals(globalMetric, other.globalMetric)
            && Objects.equals(threadMetric, other.threadMetric);
    }
}
