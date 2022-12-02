package gov.nasa.ziggy.ui.metrilyzer;

import java.io.IOException;
import java.util.Collection;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;

import gov.nasa.ziggy.metrics.MetricType;
import gov.nasa.ziggy.metrics.MetricValue;
import gov.nasa.ziggy.module.PipelineException;
import gov.nasa.ziggy.services.metrics.DeltaMetricValueGenerator;
import gov.nasa.ziggy.services.metrics.MetricsFileParser;
import gov.nasa.ziggy.util.TimeRange;

/**
 * Read metric information from a file.
 *
 * @author Sean McCauliff
 */
class FileMetricsValueSource implements MetricsValueSource {
    private final MetricsFileParser metricsFileParser;

    public FileMetricsValueSource(MetricsFileParser metricsFileParser) {
        this.metricsFileParser = metricsFileParser;
    }

    @Override
    public Map<MetricType, Collection<MetricValue>> metricValues(
        List<MetricType> selectedMetricTypes, Date windowStart, Date windowEnd) {
        Map<MetricType, Collection<MetricValue>> rv = Maps
            .newHashMapWithExpectedSize(selectedMetricTypes.size());
        Set<MetricType> typeSet = ImmutableSet.copyOf(selectedMetricTypes);
        DeltaMetricValueGenerator metricIt = null;
        try {
            metricIt = new DeltaMetricValueGenerator(metricsFileParser.parseFile());
        } catch (IOException ioe) {
            throw new PipelineException(ioe);
        }
        // TODO: could probably break this off after seeing a date
        // greater than the date we are interested in.
        for (MetricValue metricDelta : metricIt) {
            if (metricDelta.getTimestamp().before(windowStart)) {
                continue;
            }
            if (metricDelta.getTimestamp().after(windowEnd)) {
                continue;
            }
            MetricType metricType = metricDelta.getMetricType();
            if (!typeSet.contains(metricType)) {
                continue;
            }
            Collection<MetricValue> valuesForType = rv.get(metricDelta.getMetricType());
            if (valuesForType == null) {
                // Todd likes linked lists for this kind of thing.
                valuesForType = new LinkedList<>();
                rv.put(metricDelta.getMetricType(), valuesForType);
            }
            valuesForType.add(metricDelta);
        }
        return rv;
    }

    @Override
    public Map<MetricType, TimeRange> metricStartEndDates(List<MetricType> selectedMetricTypes) {
        // This actually returns a map of all the metric ranges not just
        // the ones that were asked about.
        return metricsFileParser.getTimestampRange();
    }
}
