package gov.nasa.ziggy.services.metrics;

import static gov.nasa.ziggy.services.metrics.MetricsFileParserTest.assertMetricValue;
import static org.junit.Assert.assertFalse;

import java.util.Date;
import java.util.List;

import org.junit.Test;

import com.google.common.collect.ImmutableList;

import gov.nasa.ziggy.metrics.MetricType;
import gov.nasa.ziggy.metrics.MetricValue;

/**
 * @author Sean McCauliff
 */
public class DeltaMetricValueGeneratorTest {
    @Test
    public void generateDeltaMetricValues() throws Exception {
        MetricType mt0 = new MetricType("mt0", MetricType.TYPE_COUNTER);
        MetricType mt1 = new MetricType("mt1", MetricType.TYPE_VALUE);
        Date timestampAsDate = new Date(5345345353L);
        List<MetricValue> absoluteMetricValues = ImmutableList.of(
            new MetricValue("", mt0, timestampAsDate, 1.0f),
            new MetricValue("", mt1, timestampAsDate, 2.0f),
            new MetricValue("", mt0, timestampAsDate, 5.5f),
            new MetricValue("", mt1, timestampAsDate, 2.1f));
        DeltaMetricValueGenerator generator = new DeltaMetricValueGenerator(
            absoluteMetricValues.iterator());
        MetricValue deltaMetricValue = generator.next();
        assertMetricValue(new MetricValue("", mt0, timestampAsDate, 1.0f), deltaMetricValue);
        deltaMetricValue = generator.next();
        assertMetricValue(new MetricValue("", mt1, timestampAsDate, 2.0f), deltaMetricValue);
        deltaMetricValue = generator.next();
        assertMetricValue(new MetricValue("", mt0, timestampAsDate, 4.5f), deltaMetricValue);
        deltaMetricValue = generator.next();
        assertMetricValue(new MetricValue("", mt1, timestampAsDate, .1f), deltaMetricValue);
        assertFalse(generator.hasNext());
    }
}
