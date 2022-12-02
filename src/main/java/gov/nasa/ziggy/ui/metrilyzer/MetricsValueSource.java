package gov.nasa.ziggy.ui.metrilyzer;

import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;

import gov.nasa.ziggy.metrics.MetricType;
import gov.nasa.ziggy.metrics.MetricValue;
import gov.nasa.ziggy.util.TimeRange;

/**
 * Some place to get metric values from.
 *
 * @author Sean McCauliff
 */
interface MetricsValueSource {
    /**
     * @param selectedMetricTypes a non-null set
     * @return metrics will be in increasing by time
     */
    Map<MetricType, Collection<MetricValue>> metricValues(List<MetricType> selectedMetricTypes,
        Date windowStart, Date windowEnd);

    Map<MetricType, TimeRange> metricStartEndDates(List<MetricType> selectedMetricTypes);
}
