package gov.nasa.ziggy.ui.metrilyzer;

import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Maps;

import gov.nasa.ziggy.metrics.MetricType;
import gov.nasa.ziggy.metrics.MetricValue;
import gov.nasa.ziggy.ui.proxy.MetricsLogCrudProxy;
import gov.nasa.ziggy.util.TimeRange;

/**
 * Get MetricValues from the database.
 *
 * @author Sean McCauliff
 */
public class DatabaseMetricsValueSource implements MetricsValueSource {
    @Override
    public Map<MetricType, Collection<MetricValue>> metricValues(
        List<MetricType> selectedMetricTypes, Date windowStart, Date windowEnd) {
        MetricsLogCrudProxy metricsLogCrud = new MetricsLogCrudProxy();
        Map<MetricType, Collection<MetricValue>> rv = Maps
            .newHashMapWithExpectedSize(selectedMetricTypes.size());
        for (MetricType type : selectedMetricTypes) {
            List<MetricValue> values = metricsLogCrud.retrieveAllMetricValuesForType(type,
                windowStart, windowEnd);
            rv.put(type, values);
        }
        return rv;
    }

    @Override
    public Map<MetricType, TimeRange> metricStartEndDates(List<MetricType> selectedMetricTypes) {
        MetricsLogCrudProxy metricsLogCrud = new MetricsLogCrudProxy();
        Map<MetricType, TimeRange> rv = Maps.newHashMapWithExpectedSize(selectedMetricTypes.size());
        for (MetricType type : selectedMetricTypes) {
            TimeRange interval = metricsLogCrud.getTimestampRange(type);
            rv.put(type, interval);
        }
        return rv;
    }
}
