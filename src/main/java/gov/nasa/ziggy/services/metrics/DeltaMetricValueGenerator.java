package gov.nasa.ziggy.services.metrics;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;

import gov.nasa.ziggy.metrics.MetricType;
import gov.nasa.ziggy.metrics.MetricValue;

/**
 * Given an iterator of metric values this is a new iterator of metric values that represent the
 * delta's between metric values of the same type from the same source.
 *
 * @author Sean McCauliff
 */
public class DeltaMetricValueGenerator implements Iterator<MetricValue>, Iterable<MetricValue> {
    private final Iterator<MetricValue> srcMetricValues;
    /** Maps (metric source, metric type) -> previous metric value */
    private final Map<SourceAndMetricType, Float> prevMetricSum = new HashMap<>();

    public DeltaMetricValueGenerator(Iterator<MetricValue> src) {
        srcMetricValues = src;
    }

    @Override
    public Iterator<MetricValue> iterator() {
        return this;
    }

    @Override
    public boolean hasNext() {
        return srcMetricValues.hasNext();
    }

    @Override
    public MetricValue next() {
        MetricValue nextSrc = srcMetricValues.next();
        SourceAndMetricType key = new SourceAndMetricType(nextSrc.getSource(),
            nextSrc.getMetricType());
        MetricValue rv = nextSrc;
        if (prevMetricSum.containsKey(key)) {
            float prevSum = prevMetricSum.get(key);
            float delta = nextSrc.getValue() - prevSum;
            if (delta < 0) {
                throw new IllegalStateException(
                    "delta " + delta + " is not increasing or zero for metric value " + nextSrc);
            }
            rv = new MetricValue(nextSrc.getSource(), nextSrc.getMetricType(),
                nextSrc.getTimestamp(), delta);
        }
        prevMetricSum.put(key, nextSrc.getValue());
        return rv;
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }

    private static class SourceAndMetricType {

        private final String source;
        private final MetricType metricType;

        public SourceAndMetricType(String source, MetricType metricType) {
            this.source = source;
            this.metricType = metricType;
        }

        @Override
        public int hashCode() {
            return Objects.hash(metricType, source);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if ((obj == null) || (getClass() != obj.getClass())) {
                return false;
            }
            SourceAndMetricType other = (SourceAndMetricType) obj;
            return Objects.equals(metricType, other.metricType)
                && Objects.equals(source, other.source);
        }

    }
}
