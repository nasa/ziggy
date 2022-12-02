package gov.nasa.ziggy.ui.proxy;

import java.util.Date;
import java.util.List;

import gov.nasa.ziggy.metrics.MetricType;
import gov.nasa.ziggy.metrics.MetricValue;
import gov.nasa.ziggy.metrics.MetricsCrud;
import gov.nasa.ziggy.services.security.Privilege;
import gov.nasa.ziggy.ui.ZiggyGuiConsole;
import gov.nasa.ziggy.util.TimeRange;

/**
 * @author Todd Klaus
 */
public class MetricsLogCrudProxy extends CrudProxy {
    public MetricsLogCrudProxy() {
    }

    public List<MetricType> retrieveAllMetricTypes() {
        verifyPrivileges(Privilege.PIPELINE_MONITOR);
        List<MetricType> result = ZiggyGuiConsole.crudProxyExecutor
            .executeSynchronousDatabaseTransaction(() -> {
                MetricsCrud crud = new MetricsCrud();
                List<MetricType> r = crud.retrieveAllMetricTypes();
                return r;
            });
        return result;
    }

    public List<MetricValue> retrieveAllMetricValuesForType(final MetricType metricType,
        final Date start, final Date end) {
        verifyPrivileges(Privilege.PIPELINE_MONITOR);
        List<MetricValue> result = ZiggyGuiConsole.crudProxyExecutor
            .executeSynchronousDatabaseTransaction(() -> {
                MetricsCrud crud = new MetricsCrud();
                List<MetricValue> r = crud.retrieveAllMetricValuesForType(metricType, start, end);
                return r;
            });
        return result;
    }

    public TimeRange getTimestampRange(final MetricType metricType) {
        verifyPrivileges(Privilege.PIPELINE_MONITOR);
        TimeRange result = ZiggyGuiConsole.crudProxyExecutor
            .executeSynchronousDatabaseTransaction(() -> {
                MetricsCrud crud = new MetricsCrud();
                TimeRange r = crud.getTimestampRange(metricType);
                return r;
            });
        return result;
    }
}
