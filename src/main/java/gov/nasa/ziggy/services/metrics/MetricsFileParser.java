package gov.nasa.ziggy.services.metrics;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import gov.nasa.ziggy.metrics.CounterMetric;
import gov.nasa.ziggy.metrics.MetricType;
import gov.nasa.ziggy.metrics.MetricValue;
import gov.nasa.ziggy.metrics.ValueMetric;
import gov.nasa.ziggy.util.TimeRange;
import gov.nasa.ziggy.util.io.FileUtil;

/**
 * Parses a file containing metrics that have been written with Metric.getLogString(). One per line.
 * This class is not MT-safe.
 *
 * @author Sean McCauliff
 */
public class MetricsFileParser {
    private static final int BUFFER_SIZE_BYTES = 1024 * 1024;
    private static final int TYPE_INDEX = 2;
    private static final int NAME_INDEX = 1;
    private static final int TIMESTAMP_INDEX = 0;
    private static final int VALUE_TYPE_VALUE_INDEX = TYPE_INDEX + 5;
    private static final int COUNTER_TYPE_VALUE_INDEX = TYPE_INDEX + 1;

    private final File metricsFile;
    private Map<String, MetricMetadata> metricNameToMetricType = Collections.emptyMap();
    private final String metricSource;

    public MetricsFileParser(File metricsFile) {
        this(metricsFile, "");
    }

    public MetricsFileParser(File metricsFile, String metricSource) {
        this.metricsFile = metricsFile;
        this.metricSource = metricSource;
    }

    public Iterator<MetricValue> parseFile() throws IOException {
        if (metricNameToMetricType.isEmpty()) {
            types();
        }

        return new LineIterator<>() {
            @Override
            protected MetricValue parseLine(String line) {
                String[] parts = line.split(",");
                Date timestamp = new Date(Long.parseLong(parts[TIMESTAMP_INDEX]));
                String name = parts[NAME_INDEX];
                String typeStr = parts[TYPE_INDEX];
                float value = (float) Double.parseDouble(parts[metricTypeSwitch(typeStr,
                    VALUE_TYPE_VALUE_INDEX, COUNTER_TYPE_VALUE_INDEX)]);
                MetricType metricType = metricNameToMetricType.get(name).metricType;
                return new MetricValue(metricSource, metricType, timestamp, value);
            }
        };
    }

    /**
     * Call this after calling parse to get the parsed metrics.
     *
     * @return
     * @throws IOException
     */
    public Set<MetricType> types() throws IOException {
        metricNameToMetricType = new HashMap<>();
        LineIterator<MetricMetadata> typeIt = metricMetadataIterator();
        for (MetricMetadata metadata : typeIt) {
            MetricMetadata oldMetadata = metricNameToMetricType.get(metadata.metricType.getName());
            if (oldMetadata == null) {
                metricNameToMetricType.put(metadata.metricType.getName(), metadata);
            } else {
                Date start = metadata.start.before(oldMetadata.start) ? metadata.start
                    : oldMetadata.start;
                Date end = metadata.end.after(oldMetadata.end) ? metadata.end : metadata.start;
                if (start != oldMetadata.start || end != oldMetadata.end) {
                    MetricMetadata updated = new MetricMetadata(metadata.metricType, start, end);
                    metricNameToMetricType.put(metadata.metricType.getName(), updated);
                }
            }
        }

        Set<MetricType> allTypes = Sets.newHashSetWithExpectedSize(metricNameToMetricType.size());
        for (MetricMetadata metadata : metricNameToMetricType.values()) {
            allTypes.add(metadata.metricType);
        }
        return allTypes;
    }

    public Map<MetricType, TimeRange> getTimestampRange() {
        Map<MetricType, TimeRange> rv = Maps
            .newHashMapWithExpectedSize(metricNameToMetricType.size());
        for (MetricMetadata metadata : metricNameToMetricType.values()) {
            rv.put(metadata.metricType, new TimeRange(metadata.start, metadata.end));
        }
        return rv;
    }

    private LineIterator<MetricMetadata> metricMetadataIterator() throws IOException {
        return new LineIterator<>() {
            @Override
            protected MetricMetadata parseLine(String line) {
                String[] parts = line.split(",");
                String name = parts[NAME_INDEX];
                String typeStr = parts[TYPE_INDEX];
                int type = metricTypeSwitch(typeStr, MetricType.TYPE_VALUE,
                    MetricType.TYPE_COUNTER);
                Date timestamp = new Date(Long.parseLong(parts[TIMESTAMP_INDEX]));
                MetricType metricType = new MetricType(name, type);
                return new MetricMetadata(metricType, timestamp, timestamp);
            }
        };
    }

    private <T> T metricTypeSwitch(String metricTypeStr, T valueCase, T counterCase) {
        if (metricTypeStr.equals(ValueMetric.VALUE_TYPE)) {
            return valueCase;
        }
        if (metricTypeStr.equals(CounterMetric.COUNTER_TYPE)) {
            return counterCase;
        } else {
            throw new IllegalStateException(
                "Parse error.  Unknown metric type \"" + metricTypeStr + "\".");
        }
    }

    protected Reader openReader() throws IOException {
        return new FileReader(metricsFile);
    }

    private abstract class LineIterator<T> implements Iterator<T>, Iterable<T> {
        private String nextLine;
        private final BufferedReader breader;

        LineIterator() throws IOException {
            breader = new BufferedReader(openReader(), BUFFER_SIZE_BYTES);
            nextLine = breader.readLine();
        }

        @Override
        public boolean hasNext() {
            return nextLine != null;
        }

        @Override
        public T next() {
            if (nextLine == null) {
                throw new IllegalStateException();
            }
            T rv = parseLine(nextLine);
            try {
                nextLine = breader.readLine();
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
            if (nextLine == null) {
                FileUtil.close(breader);
            }
            return rv;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Iterator<T> iterator() {
            return this;
        }

        protected abstract T parseLine(String line);
    }

    private static final class MetricMetadata {
        public final MetricType metricType;
        public final Date start;
        public final Date end;

        MetricMetadata(MetricType metricType, Date start, Date end) {
            this.metricType = metricType;
            this.start = start;
            this.end = end;
        }
    }
}
