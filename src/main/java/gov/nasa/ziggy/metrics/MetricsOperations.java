package gov.nasa.ziggy.metrics;

import java.util.Date;
import java.util.List;

import gov.nasa.ziggy.services.database.DatabaseOperations;
import gov.nasa.ziggy.util.TimeRange;

/**
 * Operations class for metrics.
 *
 * @author PT
 */
public class MetricsOperations extends DatabaseOperations {

    private MetricsCrud metricsCrud = new MetricsCrud();

    public long deleteOldMetrics(int maxRows) {
        return performTransaction(() -> metricsCrud().deleteOldMetrics(maxRows));
    }

    public List<MetricType> metricTypes() {
        return performTransaction(() -> metricsCrud().retrieveAllMetricTypes());
    }

    public List<MetricValue> metricValues(MetricType metricType, Date start, Date end) {
        return performTransaction(() -> metricsCrud().metricValues(metricType, start, end));
    }

    public TimeRange timestampRange(MetricType type) {
        return performTransaction(() -> metricsCrud().getTimestampRange(type));
    }

    /** For testing only. */
    void persist(MetricValue metricValue) {
        performTransaction(() -> metricsCrud().persist(metricValue));
    }

    /** For testing only. */
    List<Long> metricValueIds() {
        return performTransaction(() -> metricsCrud().retrieveAllMetricIds());
    }

    MetricsCrud metricsCrud() {
        return metricsCrud;
    }
}
