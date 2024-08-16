package gov.nasa.ziggy.ui.metrilyzer;

import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Maps;

import gov.nasa.ziggy.metrics.MetricType;
import gov.nasa.ziggy.metrics.MetricValue;
import gov.nasa.ziggy.metrics.MetricsOperations;
import gov.nasa.ziggy.util.TimeRange;

/**
 * Get MetricValues from the database.
 *
 * @author Sean McCauliff
 */
public class DatabaseMetricsValueSource implements MetricsValueSource {
    private final MetricsOperations metricsOperations = new MetricsOperations();

    @Override
    public Map<MetricType, Collection<MetricValue>> metricValues(
        List<MetricType> selectedMetricTypes, Date windowStart, Date windowEnd) {
        Map<MetricType, Collection<MetricValue>> rv = Maps
            .newHashMapWithExpectedSize(selectedMetricTypes.size());
        for (MetricType type : selectedMetricTypes) {
            List<MetricValue> values = metricsOperations().metricValues(type, windowStart, windowEnd);
            rv.put(type, values);
        }
        return rv;
    }

    @Override
    public Map<MetricType, TimeRange> metricStartEndDates(List<MetricType> selectedMetricTypes) {
        Map<MetricType, TimeRange> rv = Maps.newHashMapWithExpectedSize(selectedMetricTypes.size());
        for (MetricType type : selectedMetricTypes) {
            TimeRange interval = metricsOperations().timestampRange(type);
            rv.put(type, interval);
        }
        return rv;
    }

    private MetricsOperations metricsOperations() {
        return metricsOperations;
    }
}
