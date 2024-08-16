package gov.nasa.ziggy.ui.metrilyzer;

import com.google.common.collect.ImmutableSet;

import gov.nasa.ziggy.metrics.MetricsOperations;

/**
 * Get metric types from the database.
 *
 * @author Sean McCauliff
 */
@SuppressWarnings("serial")
class DatabaseMetricsTypeListModel extends MetricTypeListModel {
    private final MetricsOperations metricsOperations = new MetricsOperations();

    @Override
    public void loadMetricTypes() {
        updateTypes(ImmutableSet.copyOf(metricsOperations().metricTypes()));
    }

    private MetricsOperations metricsOperations() {
        return metricsOperations;
    }
}
