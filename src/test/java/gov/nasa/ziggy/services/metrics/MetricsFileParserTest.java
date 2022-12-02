package gov.nasa.ziggy.services.metrics;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.Date;
import java.util.Iterator;

import org.junit.Test;

import gov.nasa.ziggy.metrics.CounterMetric;
import gov.nasa.ziggy.metrics.MetricType;
import gov.nasa.ziggy.metrics.MetricValue;
import gov.nasa.ziggy.metrics.ValueMetric;

/**
 * Test the MetricsFileParser
 *
 * @author Sean McCauliff
 */
public class MetricsFileParserTest {
    @Test
    public void testParse() throws Exception {
        long timestamp = 23432434;
        Date timestampAsDate = new Date(timestamp);
        MetricType mType0 = new MetricType("vm0", MetricType.TYPE_VALUE);
        MetricType mType1 = new MetricType("vm1", MetricType.TYPE_VALUE);
        ValueMetric vm0 = ValueMetric.addValue("vm0", 1);
        ValueMetric vm1 = ValueMetric.addValue("vm1", 314);
        MetricType mType2 = new MetricType("cm0", MetricType.TYPE_COUNTER);
        CounterMetric cm0 = CounterMetric.increment("cm0");

        final StringBuilder testInput = new StringBuilder(1024);
        testInput.append(timestamp).append(',');
        vm0.toLogString(testInput);
        testInput.append('\n');
        testInput.append(timestamp).append(',');
        vm1.toLogString(testInput);
        testInput.append('\n');
        testInput.append(timestamp).append(',');
        cm0.toLogString(testInput);
        testInput.append('\n');

        MetricsFileParser parser = new MetricsFileParser(new File("bogus")) {
            @Override
            protected Reader openReader() throws IOException {
                return new StringReader(testInput.toString());
            }
        };

        Iterator<MetricValue> mvIt = parser.parseFile();
        MetricValue metricValue = mvIt.next();
        assertMetricValue(new MetricValue("", mType0, timestampAsDate, (float) vm0.getAverage()),
            metricValue);
        metricValue = mvIt.next();
        assertMetricValue(new MetricValue("", mType1, timestampAsDate, (float) vm1.getAverage()),
            metricValue);
        metricValue = mvIt.next();
        assertMetricValue(new MetricValue("", mType2, timestampAsDate, cm0.getCount()),
            metricValue);
        assertFalse(mvIt.hasNext());
    }

    static void assertMetricValue(MetricValue expected, MetricValue actual) {
        assertEquals(expected.getMetricType(), actual.getMetricType());
        assertEquals(expected.getSource(), actual.getSource());
        assertEquals(expected.getTimestamp(), actual.getTimestamp());
        assertEquals(expected.getValue(), actual.getValue(), 0.001f);
    }
}
