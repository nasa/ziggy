package gov.nasa.ziggy.ui.metrilyzer;

import gov.nasa.ziggy.services.metrics.MetricsFileParser;

/**
 * Get the metric types from the metrics file.
 *
 * @author Sean McCauliff
 */
@SuppressWarnings("serial")
class FileSourceMetricsTypeListModel extends MetricTypeListModel {
    private final MetricsFileParser metricsFileParser;

    FileSourceMetricsTypeListModel(MetricsFileParser metricsFileParser) {
        this.metricsFileParser = metricsFileParser;
    }

    @Override
    public void loadMetricTypes() {
        super.updateTypes(metricsFileParser.types());
    }
}
