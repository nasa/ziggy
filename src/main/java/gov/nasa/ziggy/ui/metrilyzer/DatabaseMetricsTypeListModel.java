package gov.nasa.ziggy.ui.metrilyzer;

import com.google.common.collect.ImmutableSet;

import gov.nasa.ziggy.ui.util.proxy.MetricsLogCrudProxy;

/**
 * Get metric types from the database.
 *
 * @author Sean McCauliff
 */
@SuppressWarnings("serial")
class DatabaseMetricsTypeListModel extends MetricTypeListModel {
    @Override
    public void loadMetricTypes() {
        MetricsLogCrudProxy metricsLogCrud = new MetricsLogCrudProxy();
        updateTypes(ImmutableSet.copyOf(metricsLogCrud.retrieveAllMetricTypes()));
    }
}
