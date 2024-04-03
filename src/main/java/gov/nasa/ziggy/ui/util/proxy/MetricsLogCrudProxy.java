package gov.nasa.ziggy.ui.util.proxy;

import java.util.Date;
import java.util.List;

import gov.nasa.ziggy.metrics.MetricType;
import gov.nasa.ziggy.metrics.MetricValue;
import gov.nasa.ziggy.metrics.MetricsCrud;
import gov.nasa.ziggy.util.TimeRange;

/**
 * @author Todd Klaus
 */
public class MetricsLogCrudProxy {
    public MetricsLogCrudProxy() {
    }

    public List<MetricType> retrieveAllMetricTypes() {
        return CrudProxyExecutor.executeSynchronousDatabaseTransaction(() -> {
            MetricsCrud crud = new MetricsCrud();
            return crud.retrieveAllMetricTypes();
        });
    }

    public List<MetricValue> retrieveAllMetricValuesForType(final MetricType metricType,
        final Date start, final Date end) {
        return CrudProxyExecutor.executeSynchronousDatabaseTransaction(() -> {
            MetricsCrud crud = new MetricsCrud();
            return crud.retrieveAllMetricValuesForType(metricType, start, end);
        });
    }

    public TimeRange getTimestampRange(final MetricType metricType) {
        return CrudProxyExecutor.executeSynchronousDatabaseTransaction(() -> {
            MetricsCrud crud = new MetricsCrud();
            return crud.getTimestampRange(metricType);
        });
    }
}
