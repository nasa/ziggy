package gov.nasa.ziggy.ui.metrilyzer;

import java.io.IOException;

import gov.nasa.ziggy.module.PipelineException;
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
        try {
            super.updateTypes(metricsFileParser.types());
        } catch (IOException e) {
            throw new PipelineException(e);
        }
    }
}
